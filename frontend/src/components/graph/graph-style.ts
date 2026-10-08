/** Shared with the 2D card. Canvas reads the same CSS theme tokens. */
export const GRAPH_CARD_WIDTH = 240;
export const GRAPH_CARD_HEIGHT = 128;
export const GRAPH_EDGE_STYLE = {
  width: 1.5,
  selectedWidth: 2,
  overviewOpacity: 0.4,
  labelFontSize: 10,
  labelHeight: 16,
  labelPaddingX: 4,
};
export function graphTheme() {
  const css = getComputedStyle(document.documentElement);
  const read = (name: string) => css.getPropertyValue(`--color-${name}`).trim();
  return {
    background: read("surface"),
    raised: read("raised"),
    border: read("border"),
    strong: read("border-strong"),
    foreground: read("fg"),
    muted: read("muted"),
    faint: read("faint"),
    accent: read("accent"),
    fontSans: css.getPropertyValue("--font-sans").trim(),
    fontMono: css.getPropertyValue("--font-mono").trim(),
  };
}
