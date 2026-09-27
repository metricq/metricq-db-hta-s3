package engine

import (
	"context"
	"fmt"
	"sort"
)

const inventoryPageBlocks = 128

// Bounds refer to offsets in the immutable data/index object, not timestamps.
// Stable bounds prevent deletions shifting every later page's records.
type objectInventoryPage struct {
	First, Last int64
	Count       int
	Ref         blob
}

func (e *Engine) loadObjectInventory(ctx context.Context, o *ObjectInfo) error {
	if len(o.Inventory) == 0 {
		return nil
	}
	if len(o.Blocks) > 0 {
		return fmt.Errorf("catalog contains both inline and paged inventory")
	}
	for start := 0; start < len(o.Inventory); start += maxQueryBlockBatch {
		end := min(start+maxQueryBlockBatch, len(o.Inventory))
		refs := make([]blob, end-start)
		for i, page := range o.Inventory[start:end] {
			if page.First < 0 || page.Last < page.First || page.Count <= 0 || page.Count > inventoryPageBlocks || page.Ref.Length <= 0 || page.Ref.Length > 32<<20 {
				return fmt.Errorf("invalid object inventory page")
			}
			if start+i > 0 && o.Inventory[start+i-1].Last >= page.First {
				return fmt.Errorf("unordered object inventory pages")
			}
			if e.catalogReadBudget > 0 && e.catalogReadBytes+page.Ref.Length > e.catalogReadBudget {
				return errCatalogBudget
			}
			e.catalogReadBytes += page.Ref.Length
			refs[i] = page.Ref
		}
		encoded, err := e.fetchEncoded(ctx, refs)
		if err != nil {
			return err
		}
		for i, b := range encoded {
			var blocks []BlockInfo
			if err := decode(b, &blocks); err != nil {
				return err
			}
			page := o.Inventory[start+i]
			if len(blocks) != page.Count {
				return fmt.Errorf("invalid inventory record count")
			}
			for j, block := range blocks {
				ref := block.Entry.Blob
				if ref.Key != o.Key || ref.Offset < page.First || ref.Offset > page.Last || ref.Length <= 0 || ref.Offset > o.Size || ref.Length > o.Size-ref.Offset || (j > 0 && blocks[j-1].Entry.Blob.Offset >= ref.Offset) {
					return fmt.Errorf("invalid object inventory block")
				}
			}
			o.Blocks = append(o.Blocks, blocks...)
		}
	}
	var live int64
	for _, block := range o.Blocks {
		live += block.Entry.Blob.Length
	}
	if live != o.LiveBytes {
		return fmt.Errorf("object inventory live-byte mismatch")
	}
	return nil
}

func inventoryObjects(pages []objectInventoryPage) map[string]bool {
	keys := map[string]bool{}
	for _, page := range pages {
		keys[page.Ref.Key] = true
	}
	return keys
}

// One pack per changed object's inventory keeps retirement local to that object.
// Pages from other objects never share this pack; unchanged pages remain live.
func (e *Engine) writeObjectInventory(ctx context.Context, o *ObjectInfo, old ObjectInfo) ([]string, error) {
	if !sort.SliceIsSorted(o.Blocks, func(i, j int) bool { return o.Blocks[i].Entry.Blob.Offset < o.Blocks[j].Entry.Blob.Offset }) {
		o.Blocks = append([]BlockInfo(nil), o.Blocks...)
		sort.Slice(o.Blocks, func(i, j int) bool { return o.Blocks[i].Entry.Blob.Offset < o.Blocks[j].Entry.Blob.Offset })
	}
	var pages []objectInventoryPage
	var p *pack
	flush := func() error {
		if p == nil {
			return nil
		}
		absent := ""
		_, err := e.put(ctx, p.key, p.buf.Bytes(), &absent)
		p = nil
		return err
	}
	write := func(blocks []BlockInfo, first, last int64) error {
		if len(blocks) == 0 {
			return nil
		}
		b, err := encode(blocks)
		if err != nil {
			return err
		}
		if len(b) > 32<<20 {
			return fmt.Errorf("inventory page exceeds memory budget")
		}
		if p != nil && p.buf.Len()+len(b) > 4<<20 {
			if err := flush(); err != nil {
				return err
			}
		}
		if p == nil {
			p, err = e.newMetadataPack("catalog")
			if err != nil {
				return err
			}
		}
		pages = append(pages, objectInventoryPage{First: first, Last: last, Count: len(blocks), Ref: p.add(b)})
		return nil
	}
	if len(old.Inventory) > 0 {
		// Retirement preserves offset order, so a single pass assigns surviving
		// descriptors to the same stable pages. Counts prove untouched pages.
		position := 0
		for _, page := range old.Inventory {
			start := position
			for position < len(o.Blocks) && o.Blocks[position].Entry.Blob.Offset <= page.Last {
				position++
			}
			blocks := o.Blocks[start:position]
			if len(blocks) == 0 {
				continue
			}
			if len(blocks) == page.Count {
				pages = append(pages, page)
			} else if err := write(blocks, min(page.First, blocks[0].Entry.Blob.Offset), page.Last); err != nil {
				return nil, err
			}
		}
		for position < len(o.Blocks) {
			blocks := o.Blocks[position:min(position+inventoryPageBlocks, len(o.Blocks))]
			if err := write(blocks, blocks[0].Entry.Blob.Offset, blocks[len(blocks)-1].Entry.Blob.Offset); err != nil {
				return nil, err
			}
			position += len(blocks)
		}
	} else if len(o.Blocks) > inventoryPageBlocks {
		for start := 0; start < len(o.Blocks); start += inventoryPageBlocks {
			blocks := o.Blocks[start:min(start+inventoryPageBlocks, len(o.Blocks))]
			if err := write(blocks, blocks[0].Entry.Blob.Offset, blocks[len(blocks)-1].Entry.Blob.Offset); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	o.Inventory = pages
	var retired []string
	live := inventoryObjects(pages)
	for key := range inventoryObjects(old.Inventory) {
		if !live[key] {
			retired = append(retired, key)
		}
	}
	return retired, nil
}

func catalogWire(n catalogNode) catalogNode {
	wire := n
	wire.Items = append([]ObjectInfo(nil), n.Items...)
	for i := range wire.Items {
		if len(wire.Items[i].Inventory) > 0 {
			wire.Items[i].Blocks = nil
		}
	}
	return wire
}
