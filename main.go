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

func deleteLine(vim *nvim.Nvim) {
	var line int
	err := vim.Eval(`line(".")`, &line)
	if err != nil {
		return
	}
	buffer, err := vim.CurrentBuffer()
	if err != nil {
		return
	}
	if line > 0 {
		vim.SetBufferLines(buffer, line-1, line, false, [][]byte{})
	}
}

func DeleteBucketOrEntry(vim *nvim.Nvim, args []string, eval string) error {
	var bktname string
	err := vim.Call("GetBoltBucket", &bktname)
	if err != nil {
		return fmt.Errorf("failed to get bolt bucket: %s", err.Error())
	}

	isEntry := HasLeadingSpace.MatchString(eval)
	// vim.Exec(fmt.Sprintf(`echom "res1 ='%s', eval='%s', hasprefix=%+v"`, bktname, eval, isEntry), false)
	if isEntry {
		val := EntryRegex.FindStringSubmatch(eval)
		if len(val) == 3 {
			key := val[1]
			// delete entry
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
				deleteLine(vim)
				return bkt.Delete([]byte(key))
			})
			// vim.Exec(fmt.Sprintf(`echom "found: [%s] => [%s]"`, key, value), false)
		} else {
			return errors.New("failed to match key => val")
		}
	} else {
		// delete bucket
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
				deleteLine(vim)
				return nil
			}

			return errors.New("bucket has entry, should delete entry first")
		})
	}
}

func CreateBucketOrEntry(vim *nvim.Nvim, args []string, eval string) error {
	var bktname string
	err := vim.Call("GetBoltBucket", &bktname)
	if err != nil {
		return fmt.Errorf("failed to get bolt bucket: %s", err.Error())
	}

	isEntry := HasLeadingSpace.MatchString(eval)
	// vim.Exec(fmt.Sprintf(`echom "res1 ='%s', eval='%s', hasprefix=%+v"`, bktname, eval, isEntry), false)
	if isEntry {
		val := EntryRegex.FindStringSubmatch(eval)
		if len(val) == 3 {
			key := val[1]
			value := val[2]
			// create entry
			db.Update(func(tx *bbolt.Tx) error {
				bktn := []byte(bktname)
				bkt := tx.Bucket([]byte(bktn))
				if bkt == nil {
					var err error
					bkt, err = tx.CreateBucketIfNotExists(bktn)
					if err != nil {
						return err
					}
				}
				return bkt.Put([]byte(key), []byte(value))
			})
			// vim.Exec(fmt.Sprintf(`echom "found: [%s] => [%s]"`, key, value), false)
		} else {
			return errors.New("failed to match key => val")
		}
	} else {
		// create bucket
		db.Update(func(tx *bbolt.Tx) error {
			bktn := []byte(bktname)
			_, err := tx.CreateBucketIfNotExists(bktn)
			return err
		})
	}
	var result string
	vim.Eval(`@"`, &result)
	vim.Exec(fmt.Sprintf(`echom "result = %s"`, result), false)
	return nil
}

func CreateBolt(vim *nvim.Nvim, args []string) error {
	buffer, err := vim.CurrentBuffer()
	if err != nil {
		return err
	}
	var boltname string
	vim.Eval(`expand("%:p")`, &boltname)

	vim.SetBufferOption(buffer, "filetype", "boltdb")
	vim.SetBufferKeyMap(buffer, "n", "bc", ":call BoltviewerCreateBucketEntry()<cr>", map[string]bool{"noremap": true})
	vim.SetBufferKeyMap(buffer, "n", "bd", ":call BoltviewerDeleteBucketEntry()<cr>", map[string]bool{"noremap": true})

	db, err = bbolt.Open(boltname, 0644, nil)
	if err != nil {
		return errors.New("failed to open bolt")
	}

	return nil
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
	vim.SetBufferLines(buffer, 0, -1, false, [][]byte{})
	vim.SetBufferKeyMap(buffer, "n", "bc", ":call BoltviewerCreateBucketEntry()<cr>", map[string]bool{"noremap": true})
	vim.SetBufferKeyMap(buffer, "n", "bd", ":call BoltviewerDeleteBucketEntry()<cr>", map[string]bool{"noremap": true})

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
	plugin.Main(func(p *plugin.Plugin) error {
		p.HandleAutocmd(&plugin.AutocmdOptions{
			Event:   `BufRead`,
			Pattern: "*.boltdb",
		}, LoadBolt)
		p.HandleAutocmd(&plugin.AutocmdOptions{
			Event:   `BufNewFile`,
			Pattern: "*.boltdb",
		}, CreateBolt)

		p.HandleFunction(&plugin.FunctionOptions{
			Name: "BoltviewerCreateBucketEntry",
			Eval: `getline(line("."))`,
		}, CreateBucketOrEntry)

		p.HandleFunction(&plugin.FunctionOptions{
			Name: "BoltviewerDeleteBucketEntry",
			Eval: `getline(line("."))`,
		}, DeleteBucketOrEntry)
		return nil
	})
}
