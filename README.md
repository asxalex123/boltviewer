# boltviewer
boltviewer is a neo-vim plugin for viewing boltdb files.

## Install
the plugin uses noevim's go-client for accessing boltdb and interacting with neovim at the same time. So, we need to install the golang executable with

```shell
go install github.com/asxalex123/boltviewer
```

This will install the go-plugin in your "$GOPATH/bin", make sure the directory is in `$PATH`

Then install the vim plugin with neovim's lazy.nvim:

```lua
-- ~/.config/nvim/lua/plugins/boltviewer.lua
return {
    {asxalex123/boltviewer.nvim},
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

the `bc` key binding is interpreted as `bolt create entry(or bucket)`, while `bd` stands for `bolt delete entry (or bucket)`. A line starts with white space is treated as an entry(which should in the form of `key => value`), and other line is treated as a bucket name.

if we hit `bc` with cursor on a bucket line, a bucket will be created in boltdb, if cursor on an entry line, an entry will be created.

if we hit `bd` with cursor on a bucket line, the bucket will be dropped if the bucket has no entry, otherwise, the action will fail.

if `bd` is hit with cursor on an entry line, the entry will be deleted from the bucket.

