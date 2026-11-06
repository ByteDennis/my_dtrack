-- Neovim 0.10+: ~/.config/nvim/init.lua. Plugin-free; mirrors the tmux Ctrl+a / vi-style habits.
vim.g.mapleader = " "                       -- Space is the leader key
vim.g.maplocalleader = " "                  -- same key for buffer-local mappings

vim.o.number = true                         -- absolute number on the cursor line
vim.o.relativenumber = true                 -- relative numbers elsewhere for quick j/k counts
vim.o.cursorline = true                     -- highlight the current line
vim.o.signcolumn = "yes"                    -- keep the gutter fixed so text never shifts
vim.o.numberwidth = 4                       -- room for 4-digit line numbers
vim.o.colorcolumn = "100"                   -- ruler at the repo's 100-column line length
vim.o.wrap = false                          -- long lines scroll instead of wrapping
vim.o.scrolloff = 3                         -- keep 3 lines visible above/below the cursor
vim.o.sidescrolloff = 5                     -- keep 5 columns visible left/right of the cursor
vim.o.termguicolors = true                  -- 24-bit colors in modern terminals
vim.o.showmode = false                      -- the statusline already shows the mode
vim.o.showtabline = 2                       -- always show the tab line
vim.o.mouse = "a"                           -- mouse works in every mode
vim.o.clipboard = "unnamedplus"             -- yank/paste through the system clipboard

vim.o.expandtab = true                      -- Tab inserts spaces
vim.o.tabstop = 4                           -- a literal tab displays as 4 columns
vim.o.shiftwidth = 4                        -- >> and << shift by 4 spaces (Python style)
vim.o.smartindent = true                    -- auto-indent new lines from context

vim.o.ignorecase = true                     -- case-insensitive search ...
vim.o.smartcase = true                      -- ... unless the pattern has an uppercase letter
vim.o.incsearch = true                      -- show matches while typing
vim.o.hlsearch = true                       -- keep matches highlighted; <Esc> clears them

vim.o.splitbelow = true                     -- :sp opens below
vim.o.splitright = true                     -- :vs opens to the right
vim.o.swapfile = false                      -- no .swp files next to sources
vim.o.undofile = true                       -- persistent undo across sessions
vim.o.updatetime = 300                      -- faster CursorHold events
vim.o.timeoutlen = 500                      -- 0.5s to finish a key sequence
vim.o.errorbells = false                    -- no beeps

vim.g.netrw_banner = 0                      -- hide the netrw help banner
vim.g.netrw_liststyle = 3                   -- tree-style file listing
vim.g.netrw_winsize = 25                    -- explorer takes 25% of the width

local map = vim.keymap.set

map("n", "<Esc>", "<cmd>nohlsearch<CR>")                     -- Esc clears search highlight
map("n", "<leader>w", "<cmd>write<CR>")                      -- Space w = save
map("n", "<leader>q", "<cmd>quit<CR>")                       -- Space q = close window
map("n", "<leader>x", "<cmd>xall<CR>")                       -- Space x = save all and quit

map("n", "vs", "<cmd>vsplit<CR>")                            -- vs = vertical split
map("n", "sp", "<cmd>split<CR>")                             -- sp = horizontal split
map("n", "<C-h>", "<C-w>h")                                  -- Ctrl+h = window left
map("n", "<C-j>", "<C-w>j")                                  -- Ctrl+j = window down
map("n", "<C-k>", "<C-w>k")                                  -- Ctrl+k = window up
map("n", "<C-l>", "<C-w>l")                                  -- Ctrl+l = window right
map("n", "<C-Up>", "<cmd>resize +2<CR>")                     -- Ctrl+Up = taller window
map("n", "<C-Down>", "<cmd>resize -2<CR>")                   -- Ctrl+Down = shorter window
map("n", "<C-Left>", "<cmd>vertical resize -2<CR>")          -- Ctrl+Left = narrower window
map("n", "<C-Right>", "<cmd>vertical resize +2<CR>")         -- Ctrl+Right = wider window

map("n", "tn", "<cmd>tabnew<CR>")                            -- tn = new tab
map("n", "tk", "<cmd>tabnext<CR>")                           -- tk = next tab
map("n", "tj", "<cmd>tabprevious<CR>")                       -- tj = previous tab
map("n", "to", "<cmd>tabonly<CR>")                           -- to = close other tabs
map("n", "<S-l>", "<cmd>bnext<CR>")                          -- Shift+l = next buffer
map("n", "<S-h>", "<cmd>bprevious<CR>")                      -- Shift+h = previous buffer

map("n", "<C-n>", "<cmd>Lexplore<CR>")                       -- Ctrl+n = toggle file tree on the left
map("n", "<C-s>", ":%s/")                                    -- Ctrl+s = start a file-wide substitute
map("n", "<leader>t", "<cmd>belowright 15split | terminal<CR>i")  -- Space t = terminal below
map("t", "<Esc>", [[<C-\><C-n>]])                            -- Esc leaves terminal insert mode

map("n", "n", "nzzzv")                                       -- next match stays centered
map("n", "N", "Nzzzv")                                       -- previous match stays centered
map("n", "<C-d>", "<C-d>zz")                                 -- half-page down stays centered
map("n", "<C-u>", "<C-u>zz")                                 -- half-page up stays centered
map("n", "J", "mzJ`z")                                       -- join lines without moving the cursor
map("v", "J", ":m '>+1<CR>gv=gv")                            -- visual J = move selection down
map("v", "K", ":m '<-2<CR>gv=gv")                            -- visual K = move selection up
map("v", "<", "<gv")                                         -- indent left and keep the selection
map("v", ">", ">gv")                                         -- indent right and keep the selection
map("x", "<leader>p", [["_dP]])                              -- paste over a selection without losing the yank
map({ "n", "v" }, "<leader>d", [["_d]])                      -- delete without overwriting the clipboard
map("n", "<leader>y", "<cmd>let @+ = expand('%:p')<CR>")     -- Space y = copy the file's absolute path

vim.api.nvim_create_autocmd("TextYankPost", {                -- briefly flash yanked text
  callback = function() vim.highlight.on_yank({ timeout = 150 }) end,
})
vim.api.nvim_create_autocmd("BufWritePre", {                 -- strip trailing whitespace on save
  callback = function()
    local view = vim.fn.winsaveview()
    vim.cmd([[keeppatterns %s/\s\+$//e]])
    vim.fn.winrestview(view)
  end,
})
vim.api.nvim_create_autocmd("FileType", {                    -- 2-space indent for web/config files
  pattern = { "lua", "json", "yaml", "javascript", "typescript", "html", "css", "markdown" },
  callback = function() vim.bo.shiftwidth = 2; vim.bo.tabstop = 2 end,
})
vim.api.nvim_create_autocmd("FileType", {                    -- wrap prose in markdown/text
  pattern = { "markdown", "text" },
  callback = function() vim.wo.wrap = true; vim.wo.linebreak = true end,
})
