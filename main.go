package main

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/neovim/go-client/nvim"
	"github.com/neovim/go-client/nvim/plugin"
	"go.etcd.io/bbolt"
)

var db *bbolt.DB

var HasLeadingSpace = regexp.MustCompile(`^[ \t]+.*$`)
var EntryRegex = regexp.MustCompile(`^[ \t]+([^ \=]*)[ \t]*\=\>[ \t]*([^ \t].*)$`)

func DeleteBucket(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("argument bucket delete length error: %d", len(args))
	}

	bktname := args[0]

	return db.Update(func(tx *bbolt.Tx) error {
		bktn := []byte(bktname)
		bkt := tx.Bucket(bktn)
		if bkt == nil {
			return nil
		}
		cursor := bkt.Cursor()
		if k, _ := cursor.First(); k == nil {
			// no more entry under the bucket
			err := tx.DeleteBucket(bktn)
			if err != nil {
				return err
			}
			return nil
		}
		return errors.New("bucket has entry, should delete entry first")
	})
}

func DeleteEntry(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("argument entry delete length error: %d", len(args))
	}
	bktname := args[0]
	key := args[1]

	return db.Update(func(tx *bbolt.Tx) error {
		bktn := []byte(bktname)
		bkt := tx.Bucket([]byte(bktn))
		if bkt == nil {
			var err error
			bkt, err = tx.CreateBucketIfNotExists(bktn)
			if err != nil {
				return err
			}
		}
		return bkt.Delete([]byte(key))
	})
}

func CreateBucket(args []string) error {
	if len(args) != 1 {
		// bucket name should be passed
		return fmt.Errorf("argument bucket length error: %d", len(args))
	}

	return db.Update(func(tx *bbolt.Tx) error {
		bktn := []byte(args[0])
		if tx.Bucket(bktn) != nil {
			return fmt.Errorf("bucket exists")
		}
		_, err := tx.CreateBucketIfNotExists(bktn)
		return err
	})
}

func CreateEntry(vim *nvim.Nvim, args []string, eval string) error {
	return createEntry(vim, args, eval, false)
}

func CreateEntryAnyway(vim *nvim.Nvim, args []string, eval string) error {
	return createEntry(vim, args, eval, true)
}


func createEntry(vim *nvim.Nvim, args []string, eval string, anyway bool) error {
	if len(args) != 3 {
		// bucket, key, value
		return fmt.Errorf("argument entry length error: %d", len(args))
	}
	bktname := args[0]
	key := args[1]
	value := args[2]

	// create entry
	return db.Update(func(tx *bbolt.Tx) error {
		bktn := []byte(bktname)
		bkt := tx.Bucket([]byte(bktn))
		if bkt == nil {
			var err error
			bkt, err = tx.CreateBucketIfNotExists(bktn)
			if err != nil {
				return err
			}
		}
		if !anyway {
			if len(bkt.Get([]byte(key))) != 0 {
				return errors.New("key exists")
			}
		}

		return bkt.Put([]byte(key), []byte(value))
	})
	// vim.Exec(fmt.Sprintf(`echom "found: [%s] => [%s]"`, key, value), false)
}

func LoadBolt(vim *nvim.Nvim, args []string) error {
	buffer, err := vim.CurrentBuffer()
	if err != nil {
		return err
	}
	vim.SetBufferOption(buffer, "filetype", "boltdb")
	var boltname string
	vim.Eval(`expand("%:p")`, &boltname)

	vim.SetBufferOption(buffer, "buftype", "nofile")
	// clear lines
	vim.SetBufferLines(buffer, 0, -1, false, [][]byte{})
	//
	// vim.SetBufferKeyMap(buffer, "n", "bc", ":call BoltviewerCreateBucketEntry()<cr>", map[string]bool{"noremap": true})
	// vim.SetBufferKeyMap(buffer, "n", "bd", ":call BoltviewerDeleteBucketEntry()<cr>", map[string]bool{"noremap": true})

	db, err = bbolt.Open(boltname, 0644, nil)
	if err != nil {
		return errors.New("failed to open bolt")
	}

	return db.View(func(tx *bbolt.Tx) error {
		start := 0
		end := 1
		tx.ForEach(func(name []byte, bkt *bbolt.Bucket) error {
			end += 1
			lines := [][]byte{name}
			cursor := bkt.Cursor()
			for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
				lines = append(lines, []byte(fmt.Sprintf("\t%s => %s", k, v)))
				end += 1
			}
			vim.SetBufferLines(buffer, start, end+1, false, lines)
			start = end + 1
			end = start + 1
			return nil
		})
		return nil
	})
	// return "Hello " + boltname + ": " + strings.Join(args, ","), nil
}

func main() {
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	plugin.Main(func(p *plugin.Plugin) error {
		p.HandleFunction(&plugin.FunctionOptions{
			Name: "BoltviewerLoad",
		}, LoadBolt)

		p.HandleFunction(&plugin.FunctionOptions{
			Name: "BoltviewerCreateBucket",
		}, CreateBucket)

		p.HandleFunction(&plugin.FunctionOptions{
			Name: "BoltviewerCreateEntry",
		}, CreateEntry)

		p.HandleFunction(&plugin.FunctionOptions{
			Name: "BoltviewerCreateEntryAnyway",
		}, CreateEntryAnyway)

		p.HandleFunction(&plugin.FunctionOptions{
			Name: "BoltviewerDeleteEntry",
		}, DeleteEntry)

		p.HandleFunction(&plugin.FunctionOptions{
			Name: "BoltviewerDeleteBucket",
		}, DeleteBucket)
		return nil
	})
}
