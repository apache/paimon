This README gives an overview of how to build and contribute to the
documentation of Paimon.

The documentation is included with the source of Paimon in order to ensure
that you always have docs corresponding to your checked-out version.

# Requirements

### Build the site locally

Make sure you have installed Hugo on your system.
Note: An extended version of Hugo <= 0.124.1 is required. you can Find this at [Hugo](https://github.com/gohugoio/hugo/releases/tag/v0.124.1)

From this directory:

  * Fetch the theme submodule
	```sh
	git submodule update --init --recursive
	```
  * Start local server
	```sh
	hugo -b "" serve
	```

The site can be viewed at http://localhost:1313/

# Contribute

## Markdown

The documentation pages are written in
[Markdown](http://daringfireball.net/projects/markdown/syntax). It is possible
to use [GitHub flavored
syntax](http://github.github.com/github-flavored-markdown) and intermix plain
html.

## Front matter

In addition to Markdown, every page contains a Jekyll front matter, which
specifies the title of the page and the layout to use. The title is used as the
top-level heading for the page.

    ---
    title: "Title of the Page"
    ---

    ---
    title: "Title of the Page" <-- Title rendered in the side nav
    weight: 1 <-- Weight controls the ordering of pages in the side nav
    type: docs <-- required
    aliases:  <-- Alias to setup redirect from removed page to this one
      - /alias/to/removed/page.html
    ---

## Structure

### Page

#### Headings

All documents are structured with headings. From these headings, you can
automatically generate a page table of contents (see below).

```
# Level-1 Heading  <- Used for the title of the page 
## Level-2 Heading <- Start with this one for content
### Level-3 heading
#### Level-4 heading
##### Level-5 heading
```

Please stick to the "logical order" when using the headlines, e.g. start with
level-2 headings and use level-3 headings for subsections, etc. Don't use a
different ordering, because you don't like how a headline looks.

#### Table of Contents

Table of contents are added automatically to every page, based on heading levels
2 - 4. The ToC can be omitted by adding the following to the front matter of
the page:

    ---
    bookToc: false
    ---

### ShortCodes 

Paimon uses [shortcodes](https://gohugo.io/content-management/shortcodes/) to add
custom functionality to its documentation markdown.

Its implementation and documentation can be found at
`./layouts/shortcodes/artifact.html`. Please refer to `./layouts/shortcodes/`
for other shortcodes available.
