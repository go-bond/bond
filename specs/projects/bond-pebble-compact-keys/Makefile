PANDOC ?= pandoc
OPEN ?= xdg-open
HTML_DIR := html
PANDOC_CSS_FILE ?= /tmp/pandoc-style.css
SOURCES := $(wildcard *.md)
HTML := $(patsubst %.md,$(HTML_DIR)/%.html,$(SOURCES))

.PHONY: all html view clean

all: html

html: $(HTML)

view: html
	@for file in $(HTML); do \
		$(OPEN) "$$file"; \
	done

$(HTML_DIR)/%.html: %.md Makefile
	mkdir -p "$(@D)"
	printf '%s\n' "$$PANDOC_CSS" > "$(PANDOC_CSS_FILE)"
	$(PANDOC) --standalone --embed-resources --from=markdown --to=html5 --css="$(PANDOC_CSS_FILE)" --metadata title="$(basename $<)" --output="$@" "$<"

clean:
	rm -rf $(HTML_DIR) "$(PANDOC_CSS_FILE)"

define PANDOC_CSS_SEPIA
/* ---- pandoc sepia stylesheet ------------------------------------------
   A warm, paper-like reading theme.
   Use with:  pandoc input.md -s -c style-sepia.css -o output.html
   Self-contained build:  add --embed-resources to inline this into the HTML
   Tip: pair with --no-highlight so code stays in the warm monochrome palette
   ----------------------------------------------------------------------- */

:root {
  --paper:    #f4ecd8;  /* page background (classic sepia) */
  --paper-2:  #ece0c8;  /* code + table-header background */
  --ink:      #5b4636;  /* body text (warm brown) */
  --muted:    #8a7256;  /* blockquotes, captions */
  --rule:     #d8c6a0;  /* hairlines */
  --h1:       #7c2d12;  /* deep rust / mahogany */
  --h2:       #92400e;  /* burnt amber */
  --h3:       #a16207;  /* ochre */
  --link:     #9a3412;  /* warm sienna */
  --maxw:     42rem;
}

html { font-size: 18px; }

body {
  font-family: "Iowan Old Style", "Palatino Linotype", Palatino, Georgia,
               "Times New Roman", serif;
  color: var(--ink);
  background: var(--paper);
  line-height: 1.7;
  max-width: var(--maxw);
  margin: 3rem auto;
  padding: 0 1.5rem;
  -webkit-font-smoothing: antialiased;
}

/* ---- headings: warm, ranked tones ------------------------------------ */
h1, h2, h3, h4 { line-height: 1.3; font-weight: 700; }

h1 {
  color: var(--h1);
  font-size: 2.1rem;
  margin: 0 0 1.2rem;
  padding-bottom: .3rem;
  border-bottom: 2px solid var(--h1);
}

h2 {
  color: var(--h2);
  font-size: 1.55rem;
  margin: 2.4rem 0 .8rem;
  padding-bottom: .25rem;
  border-bottom: 1px solid var(--rule);
}

h3 { color: var(--h3); font-size: 1.25rem; margin: 1.8rem 0 .6rem; }
h4 { color: var(--muted); font-size: 1.05rem; font-style: italic;
     letter-spacing: .02em; margin: 1.5rem 0 .5rem; }

/* ---- links ----------------------------------------------------------- */
a { color: var(--link); text-decoration: none;
    border-bottom: 1px solid rgba(154, 52, 18, .35); }
a:hover { border-bottom-color: var(--link); }

/* ---- code ------------------------------------------------------------ */
code {
  font-family: "SF Mono", "Cascadia Code", Menlo, Consolas, ui-monospace, monospace;
  font-size: .88em;
  background: var(--paper-2);
  padding: .12em .4em;
  border-radius: 4px;
}

pre {
  background: var(--paper-2);
  border: 1px solid var(--rule);
  border-left: 4px solid var(--h3);
  border-radius: 6px;
  padding: 1rem 1.1rem;
  overflow-x: auto;
  line-height: 1.5;
  color: var(--ink);
}
pre code { background: none; padding: 0; }

/* ---- blockquotes ----------------------------------------------------- */
blockquote {
  margin: 1.3rem 0;
  padding: .3rem 1.2rem;
  color: var(--muted);
  font-style: italic;
  border-left: 4px solid var(--h3);
}

/* ---- tables ---------------------------------------------------------- */
table { border-collapse: collapse; width: 100%; margin: 1.5rem 0; }
th, td { border: 1px solid var(--rule); padding: .5rem .8rem; text-align: left; }
th { background: var(--paper-2); font-weight: 700; }
tr:nth-child(even) td { background: rgba(216, 198, 160, .22); }

/* ---- misc ------------------------------------------------------------ */
hr { border: none; border-top: 1px solid var(--rule); margin: 2.5rem 0; }
img { max-width: 100%; }

/* A gently darker "night sepia" for dark-mode systems */
@media (prefers-color-scheme: dark) {
  :root {
    --paper: #2b2218; --paper-2: #352a1d; --ink: #e6d7bb;
    --muted: #b9a583; --rule: #4a3d2a;
    --h1: #e08a5b; --h2: #d9a441; --h3: #cbb06a; --link: #e09a6b;
  }
}
endef

define PANDOC_CSS_SEPIA2
/* ---- pandoc Warm Mocha (dark) -----------------------------------------
   Cozy dark theme built around a milk-chocolate brown core (#5F4B32),
   with a muted, earthy accent palette — caramel, sage, gold, dusty rose,
   dusty teal — and warm near-white text.
   Carries over: 10% page zoom, serif reading font, the --text knob.
   Pair with the matching syntax theme for coloured code:
     pandoc input.md -s -c style-mocha-dark.css --embed-resources \
            --highlight-style=mocha-dark.theme -o output.html
   ----------------------------------------------------------------------- */

/* OPTIONAL: Literata (Google's e-reader font); add "Literata", to --read-font */
/* @import url('https://fonts.googleapis.com/css2?family=Literata:ital,opsz,wght@0,7..72,400;0,7..72,600;1,7..72,400&display=swap'); */

:root {
  /* core + warm dark base */
  --bg:      #4c3c29;  /* milk-chocolate brown core           */
  --bg-soft: #553f28;  /* code / table-header bg (inset)      */
  --gray:    #c0ae90;  /* muted / comments (warm taupe)       */
  /* muted earthy accents (calm on the brown) */
  --rust:    #dda06a;  /* caramel amber (h1, operators)       */
  --ochre:   #dcc079;  /* honey gold (h3, types)              */
  --teal:    #a8b56f;  /* sage / olive (h2) — warm, not minty */
  --blue:    #86aeb0;  /* dusty teal-blue (links, vars)       */
  --green:   #a8b56f;  /* sage (strings)                      */
  --purple:  #cf99a6;  /* dusty rose (numbers)                */
  --red:     #e08a76;  /* muted coral (keywords)              */

  /* Body reading text — warm near-white (kept from before).
     Purer white?  #ffffff or #fbf7ec.   Warmer/softer?  #ebdbb2 ... #ddcaa0. */
  --text:    #f6efda;

  --rule: rgba(192,174,144,.32);   /* warm hairline */
  --maxw: 44rem;                   /* measure (scales with the zoom) */

  --read-font: Charter, "Bitstream Charter", "Iowan Old Style",
               "Source Serif 4", "Source Serif Pro", Georgia,
               "Times New Roman", serif;
  --mono-font: "SF Mono", "Cascadia Code", Menlo, Consolas, ui-monospace, monospace;
}

/* ---- the 10% zoom (base 17px × 1.1; change 1.1 to taste) -------------- */
html { font-size: calc(17px * 1.1); }

body {
  font-family: var(--read-font);
  color: var(--text);
  background: var(--bg);
  line-height: 1.7;
  max-width: var(--maxw);
  margin: 3rem auto;
  padding: 0 1.25rem;
  -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility;
}

/* ---- headings: warm-led, ranked -------------------------------------- */
h1, h2, h3, h4 { line-height: 1.25; font-weight: 700; }

h1 {
  color: var(--rust);
  font-size: 2.05rem;
  margin: 0 0 1.2rem;
  padding-bottom: .3rem;
  border-bottom: 3px solid var(--rust);
}

h2 {
  color: var(--teal);
  font-size: 1.5rem;
  margin: 2.4rem 0 .8rem;
  padding-bottom: .25rem;
  border-bottom: 1px solid var(--rule);
}

h3 { color: var(--ochre); font-size: 1.2rem;  margin: 1.8rem 0 .6rem; }
h4 { color: var(--gray);  font-size: 1.02rem; text-transform: uppercase;
     letter-spacing: .04em; margin: 1.5rem 0 .5rem; }

/* ---- links ----------------------------------------------------------- */
a { color: var(--blue); text-decoration: none;
    border-bottom: 1px solid rgba(134,174,176,.4); }
a:hover { border-bottom-color: var(--blue); }

/* ---- code ------------------------------------------------------------ */
code {
  font-family: var(--mono-font);
  font-size: .88em;
  background: var(--bg-soft);
  padding: .12em .4em;
  border-radius: 4px;
}

/* pandoc wraps highlighted code as div.sourceCode > pre.sourceCode */
pre, div.sourceCode {
  background: var(--bg-soft);
  border: 1px solid var(--rule);
  border-left: 4px solid var(--rust);
  border-radius: 6px;
}
pre {
  padding: 1rem 1.1rem;
  overflow-x: auto;
  line-height: 1.45;
  color: var(--text);
}
div.sourceCode { padding: 0; }
div.sourceCode pre { border: none; border-radius: 0; background: none; }
pre code, code.sourceCode { background: none; padding: 0; font-size: 1em; }

/* ---- blockquotes ----------------------------------------------------- */
blockquote {
  margin: 1.2rem 0;
  padding: .2rem 1.1rem;
  color: var(--gray);
  font-style: italic;
  border-left: 4px solid var(--teal);
}

/* ---- tables ---------------------------------------------------------- */
table { border-collapse: collapse; width: 100%; margin: 1.4rem 0; }
th, td { border: 1px solid var(--rule); padding: .5rem .75rem; text-align: left; }
th { background: var(--bg-soft); color: var(--text); font-weight: 700; }
tr:nth-child(even) td { background: rgba(214,196,161,.08); }

/* ---- misc ------------------------------------------------------------ */
hr { border: none; border-top: 1px solid var(--rule); margin: 2.5rem 0; }
img { max-width: 100%; }

/* Dark theme by design — no dark-mode @media block needed. */
endef

define PANDOC_CSS_SOLARIZED
/* ---- pandoc Solarized Light (reading edition) -------------------------
   Ethan Schoonover's Solarized palette · warm #fdf6e3 background.
   • Whole page zoomed 10% (font + width + spacing) via the root size below.
   • Body set in a humanist serif reading stack (Charter → Georgia).
   Pair with the matching syntax theme for coloured code:
     pandoc input.md -s -c style-solarized-light.css --embed-resources \
            --highlight-style=solarized-light.theme -o output.html
   ----------------------------------------------------------------------- */

/* OPTIONAL: uncomment to load Literata (Google's e-reader font) and add it
   to the front of --read-font below. Needs an internet connection to load. */
/* @import url('https://fonts.googleapis.com/css2?family=Literata:ital,opsz,wght@0,7..72,400;0,7..72,600;1,7..72,400&display=swap'); */

:root {
  /* Solarized base tones (light) */
  --base3:  #fdf6e3;  /* page background        */
  --base2:  #eee8d5;  /* code / table-header bg */
  --base1:  #93a1a1;  /* muted / secondary      */
  --base00: #bbcccc;  /* body text              */
  --base01: #586e75;  /* emphasized text        */
  /* Body reading text. Lightened toward white for a softer look.
     Want it whiter?  push toward #b8c0c0 (very soft) ... #cdd4d4 (near white).
     Too faint?       pull back to #7c8e8e, or #657b83 for the original.   */
  --text:   #bbcccc;
  /* Accents */
  --yellow: #b58900;  --orange: #cb4b16;  --red:    #dc322f;
  --magenta:#d33682;  --violet: #6c71c4;  --blue:   #268bd2;
  --cyan:   #2aa198;  --green:  #859900;

  --rule: rgba(147,161,161,.45);   /* base1 hairline */
  --maxw: 44rem;                   /* measure (scales with the zoom) */

  /* the reading face — add "Literata", at the front if you enabled the @import */
  --read-font: Charter, "Bitstream Charter", "Iowan Old Style",
               "Source Serif 4", "Source Serif Pro", Georgia,
               "Times New Roman", serif;
  --mono-font: "SF Mono", "Cascadia Code", Menlo, Consolas, ui-monospace, monospace;
}

/* ---- the 10% zoom ----------------------------------------------------- */
/* Base design size is 17px; × 1.1 zooms the whole page. Change 1.1 to taste
   (1.0 = original, 1.2 = +20%); everything below is rem/em-based so it all
   scales together — font, column width, and spacing. */
html { font-size: calc(17px * 1.1); }

body {
  font-family: var(--read-font);
  color: var(--text);
  background: var(--base3);
  line-height: 1.7;
  max-width: var(--maxw);
  margin: 3rem auto;
  padding: 0 1.25rem;
  -webkit-font-smoothing: antialiased;
  text-rendering: optimizeLegibility;
}

/* ---- headings: Solarized accents, ranked ----------------------------- */
h1, h2, h3, h4 { line-height: 1.25; font-weight: 700; }

h1 {
  color: var(--blue);
  font-size: 2.05rem;
  margin: 0 0 1.2rem;
  padding-bottom: .3rem;
  border-bottom: 3px solid var(--blue);
}

h2 {
  color: var(--cyan);
  font-size: 1.5rem;
  margin: 2.4rem 0 .8rem;
  padding-bottom: .25rem;
  border-bottom: 1px solid var(--rule);
}

h3 { color: var(--violet); font-size: 1.2rem;  margin: 1.8rem 0 .6rem; }
h4 { color: var(--base01); font-size: 1.02rem; text-transform: uppercase;
     letter-spacing: .04em; margin: 1.5rem 0 .5rem; }

/* ---- links ----------------------------------------------------------- */
a { color: var(--blue); text-decoration: none; }
a:hover { text-decoration: underline; }

/* ---- code ------------------------------------------------------------ */
code {
  font-family: var(--mono-font);
  font-size: .88em;
  background: var(--base2);
  padding: .12em .4em;
  border-radius: 4px;
}

/* pandoc wraps highlighted code as div.sourceCode > pre.sourceCode */
pre, div.sourceCode {
  background: var(--base2);
  border: 1px solid var(--rule);
  border-left: 4px solid var(--blue);
  border-radius: 6px;
}
pre {
  padding: 1rem 1.1rem;
  overflow-x: auto;
  line-height: 1.45;
  color: var(--text);
}
div.sourceCode { padding: 0; }
div.sourceCode pre { border: none; border-radius: 0; background: none; }
pre code, code.sourceCode { background: none; padding: 0; font-size: 1em; }

/* ---- blockquotes ----------------------------------------------------- */
blockquote {
  margin: 1.2rem 0;
  padding: .2rem 1.1rem;
  color: var(--base1);
  font-style: italic;
  border-left: 4px solid var(--cyan);
}

/* ---- tables ---------------------------------------------------------- */
table { border-collapse: collapse; width: 100%; margin: 1.4rem 0; }
th, td { border: 1px solid var(--rule); padding: .5rem .75rem; text-align: left; }
th { background: var(--base2); color: var(--base01); font-weight: 700; }
tr:nth-child(even) td { background: rgba(238,232,213,.5); }

/* ---- misc ------------------------------------------------------------ */
hr { border: none; border-top: 1px solid var(--rule); margin: 2.5rem 0; }
img { max-width: 100%; }

/* ---- Solarized Dark for dark-mode systems ---------------------------- */
@media (prefers-color-scheme: dark) {
  :root {
    --base3:  #002b36;  /* base03 */
    --base2:  #073642;  /* base02 */
    --base00: #839496;  /* base0  */
    --base01: #93a1a1;  /* base1  */
    --base1:  #586e75;  /* base01 */
    --rule:   rgba(88,110,117,.6);
  }
}
endef

export PANDOC_CSS=$(PANDOC_CSS_SEPIA2)
