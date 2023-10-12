# boltviewer
boltviewer is a neo-vim plugin for viewing boltdb files.

## Install
the plugin uses noevim's go-client for accessing boltdb and interacting with neovim at the same time. So, we need to install the golang executable with

```shell
go install github.com/asxalex123/boltviewer@main
```

This will install the go-plugin in your "$GOPATH/bin", make sure the directory is in `$PATH`

Then install the vim plugin with neovim's lazy.nvim:

```lua
-- ~/.config/nvim/lua/plugins/boltviewer.lua
return {
    {"asxalex123/boltviewer.nvim"},
}
```

Start nvim and lazy.nvim will do the rest.

## keybindings
After installed, while open or create file with suffix `.boltdb`, the plugin will show the content in the format below:

```
bucket-name1
    key1 => value1
    key2 => value2
bucket-name2
    key3 => value3
    key4 => value4
```

The `<leader>bib` stands for `bolt insert bucket`, while `<leader>bie` stands for `bolt insert entry`.

`<leader>bm` represents `bolt modify`, this modifies the entry under the current cursor.

if we hit `<leader>bd` with cursor on a bucket line, the bucket will be dropped if the bucket has no entry, otherwise, the action will fail.

if `<leader>bd` is hit with cursor on an entry line(a line starts with `\t` or space, and the line has the format of `key => value`), the entry will be deleted from the bucket.

