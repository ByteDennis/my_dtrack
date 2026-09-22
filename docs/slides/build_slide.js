const pptxgen = require("pptxgenjs");
const fs = require("fs");
const React = require("react");
const { renderToStaticMarkup } = require("react-dom/server");
const sharp = require("sharp");
const fa = require("react-icons/fa");
const si = require("react-icons/si");
const gr = require("react-icons/gr");
const pi = require("react-icons/pi");

const NAVY = "1E2761", ICE = "E8F0FC", ICE2 = "CADCFC", INK = "1F2933", MUTED = "6B7280", RED = "C0392B", WHITE = "FFFFFF", CARD = "F4F6FA", DBX = "FF3621";
const FONT = "Calibri";

// >>> render a react-icon to a base64 PNG data string for addImage <<< //
async function icon(Component, color, px = 320) {
  const svg = renderToStaticMarkup(React.createElement(Component, { color: `#${color}`, size: px }));
  const buf = await sharp(Buffer.from(svg)).png().toBuffer();
  return "image/png;base64," + buf.toString("base64");
}

async function main() {
  const pres = new pptxgen();
  pres.layout = "LAYOUT_WIDE";
  const s = pres.addSlide();
  s.background = { color: WHITE };

  s.addText("Databricks Migration Quality Check", { x: 0.5, y: 0.3, w: 12.3, h: 0.6, fontFace: FONT, fontSize: 30, bold: true, color: NAVY, margin: 0, isTextBox: true });
  s.addText("Framework 3.0 (PoC)  ·  left side stays on-prem, everything else runs in Databricks", { x: 0.5, y: 0.9, w: 12.3, h: 0.35, fontFace: FONT, fontSize: 14, color: MUTED, margin: 0, isTextBox: true });

  const TOP = 1.5, H = 2.85;

  // LEFT box
  const LX = 0.5, LW = 2.9;
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: LX, y: TOP, w: LW, h: H, fill: { color: NAVY }, line: { color: NAVY }, rectRadius: 0.12 });
  s.addText("LEFT", { x: LX + 0.2, y: TOP + 0.15, w: LW - 0.4, h: 0.4, fontFace: FONT, fontSize: 16, bold: true, color: WHITE, margin: 0, isTextBox: true });
  s.addText("on-prem PC", { x: LX + 0.2, y: TOP + 0.5, w: LW - 0.4, h: 0.28, fontFace: FONT, fontSize: 10, italic: true, color: ICE2, margin: 0, isTextBox: true });
  ["count", "sample", "stats", "push"].forEach((t, i) => {
    const y = TOP + 0.9 + i * 0.46;
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: LX + 0.2, y, w: LW - 0.4, h: 0.38, fill: { color: "2E3A7A" }, line: { color: "3D4C99" }, rectRadius: 0.06 });
    s.addText(`${i + 1}  ${t}`, { x: LX + 0.32, y, w: LW - 0.6, h: 0.38, fontFace: FONT, fontSize: 12, color: WHITE, margin: 0, valign: "middle", isTextBox: true });
  });

  // sources below LEFT: one stem into one pill holding the four source glyphs
  const stemX = LX + LW / 2;
  const pillY = TOP + H + 0.4, pillH = 0.8;
  s.addShape(pres.shapes.LINE, { x: stemX, y: TOP + H, w: 0, h: 0.4, line: { color: NAVY, width: 1.5, endArrowType: "triangle" } });
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: LX, y: pillY, w: LW, h: pillH, fill: { color: CARD }, line: { color: "E5E7EB" }, rectRadius: 0.4 });
  const srcIcons = [
    [gr.GrOracle, "C74634"],
    ["SAS", "1E62A8"],
    [fa.FaHive, "B8860B"],
    [pi.PiMicrosoftExcelLogoFill, "1D6F42"],
  ];
  const iconW = 0.52, slot = LW / srcIcons.length;
  for (let i = 0; i < srcIcons.length; i++) {
    const cx = LX + slot * i + slot / 2;
    const [glyph, color] = srcIcons[i];
    if (typeof glyph === "string") {
      s.addText(glyph, { x: cx - iconW / 2, y: pillY + (pillH - iconW) / 2, w: iconW, h: iconW, fontFace: "Arial", fontSize: 13, bold: true, color, align: "center", valign: "middle", margin: 0, isTextBox: true });
    } else {
      s.addImage({ data: await icon(glyph, color), x: cx - iconW / 2, y: pillY + (pillH - iconW) / 2, w: iconW, h: iconW });
    }
  }

  // arrow LEFT -> S3
  const AY = TOP + H / 2 - 0.25;
  s.addShape(pres.shapes.RIGHT_ARROW, { x: LX + LW + 0.12, y: AY, w: 0.95, h: 0.5, fill: { color: ICE2 }, line: { color: ICE2 } });
  s.addText("stats only", { x: LX + LW + 0.12, y: AY - 0.32, w: 0.95, h: 0.28, fontFace: FONT, fontSize: 9.5, bold: true, color: NAVY, align: "center", margin: 0, isTextBox: true });

  // S3 cylinder
  const SX = 4.6, SW = 4.35;
  s.addShape(pres.shapes.CAN, { x: SX, y: TOP, w: SW, h: H, fill: { color: ICE }, line: { color: ICE2, width: 1.5 } });
  s.addText("S3", { x: SX, y: TOP + 0.2, w: SW, h: 0.4, fontFace: FONT, fontSize: 16, bold: true, color: NAVY, align: "center", margin: 0, isTextBox: true });
  const shelfX = SX + 0.35, shelfW = SW - 0.7;
  function shelf(y, h, title, body) {
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: shelfX, y, w: shelfW, h, fill: { color: WHITE }, line: { color: ICE2 }, rectRadius: 0.06 });
    s.addText([
      { text: title, options: { bold: true, color: NAVY, fontSize: 11.5, breakLine: true } },
      { text: body, options: { color: INK, fontSize: 10 } },
    ], { x: shelfX + 0.12, y, w: shelfW - 0.24, h, fontFace: FONT, margin: 0, valign: "middle", isTextBox: true });
  }
  shelf(TOP + 0.72, 0.6, "Stage 1 · row counts per day", "exact count, one query per side → verdict");
  s.addShape(pres.shapes.RECTANGLE, { x: shelfX, y: TOP + 1.4, w: shelfW, h: 0.5, fill: { color: RED }, line: { color: RED } });
  s.addText("mismatch → reported\nmatch → filtered in, enter Stage 2", { x: shelfX, y: TOP + 1.4, w: shelfW, h: 0.5, fontFace: FONT, fontSize: 10, bold: true, color: WHITE, align: "center", valign: "middle", margin: 0, isTextBox: true });
  shelf(TOP + 1.98, 0.6, "Stage 2 · column stats", "deterministic, efficient, unbiased sampling");

  // arrow S3 <-> RIGHT (auto job)
  s.addShape(pres.shapes.LEFT_RIGHT_ARROW, { x: SX + SW + 0.12, y: AY, w: 0.95, h: 0.5, fill: { color: DBX }, line: { color: DBX } });
  s.addText("auto job", { x: SX + SW + 0.12, y: AY - 0.32, w: 0.95, h: 0.28, fontFace: FONT, fontSize: 9.5, bold: true, color: DBX, align: "center", margin: 0, isTextBox: true });

  // RIGHT box: Databricks
  const RX = 10.02, RW = 2.81;
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: RX, y: TOP, w: RW, h: H, fill: { color: NAVY }, line: { color: NAVY }, rectRadius: 0.12 });
  s.addText("RIGHT", { x: RX + 0.2, y: TOP + 0.15, w: RW - 0.4, h: 0.4, fontFace: FONT, fontSize: 16, bold: true, color: WHITE, margin: 0, isTextBox: true });
  s.addText("Databricks job, started once per run", { x: RX + 0.2, y: TOP + 0.5, w: RW - 0.4, h: 0.28, fontFace: FONT, fontSize: 10, italic: true, color: ICE2, margin: 0, isTextBox: true });
  s.addImage({ data: await icon(si.SiDatabricks, DBX), x: RX + RW / 2 - 0.5, y: TOP + 0.9, w: 1.0, h: 1.0 });
  s.addText([
    { text: "pull", options: { bold: true } }, { text: "  ·  " },
    { text: "compute", options: { bold: true } }, { text: "  ·  " },
    { text: "compare", options: { bold: true } },
  ], { x: RX + 0.1, y: TOP + 1.98, w: RW - 0.2, h: 0.35, fontFace: FONT, fontSize: 13, color: WHITE, align: "center", margin: 0, isTextBox: true });
  s.addText("table or Volume · reads S3 directly", { x: RX + 0.1, y: TOP + 2.35, w: RW - 0.2, h: 0.3, fontFace: FONT, fontSize: 9.5, color: ICE2, align: "center", margin: 0, isTextBox: true });

  // bottom left: five punchlines with yellow ball bullets, titles only
  const BY = TOP + H + 0.35, BH = 7.2 - BY;
  const punch = [
    "Auto queue",
    "Stats travel, rows don't",
    "One job start per run",
    "Resume, don't restart",
    "Excluded days stay excluded",
  ];
  const PY0 = pillY + pillH + 0.22, PSTEP = 0.31, BALL = 0.24;
  punch.forEach((t, i) => {
    const y = PY0 + i * PSTEP;
    s.addShape(pres.shapes.OVAL, { x: LX + 0.05, y: y + (PSTEP - BALL) / 2, w: BALL, h: BALL, fill: { color: "F5C518" }, line: { color: "000000", width: 1 } });
    s.addText(String(i + 1), { x: LX + 0.05, y: y + (PSTEP - BALL) / 2, w: BALL, h: BALL, fontFace: FONT, fontSize: 9, bold: true, color: "000000", align: "center", valign: "middle", margin: 0, isTextBox: true });
    s.addText(t, { x: LX + 0.42, y, w: 3.4, h: PSTEP, fontFace: FONT, fontSize: 12, bold: true, color: NAVY, valign: "middle", margin: 0, isTextBox: true });
  });

  // bottom right: file tree, taller
  const TX = 7.9, TW = 12.83 - TX;
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: TX, y: BY, w: TW, h: BH, fill: { color: ICE }, line: { color: ICE2 }, rectRadius: 0.08 });
  s.addText("What lands in S3", { x: TX + 0.2, y: BY + 0.12, w: TW - 0.4, h: 0.3, fontFace: FONT, fontSize: 12.5, bold: true, color: NAVY, margin: 0, isTextBox: true });
  const tree = [
    "s3://work/runs/<run_id>/",
    "├─ stage1/<pair>.json       verdict per day",
    "├─ manifest/<pair>.json     admitted days",
    "├─ units/<unit>.json        left-side stats",
    "├─ _INDEX/<n>-<unit>.json   ready pointers",
    "├─ results/<unit>.json      comparisons",
    "└─ dead/<unit>.json         failed units",
    "s3://report/",
    "├─ history/<run_id>/        summary.json",
    "│                           index.html",
    "│                           report.xlsx",
    "└─ latest/pointer.json      newest complete run",
  ].join("\n");
  s.addText(tree, { x: TX + 0.2, y: BY + 0.45, w: TW - 0.4, h: BH - 0.55, fontFace: "Courier New", fontSize: 9, color: INK, margin: 0, valign: "top", isTextBox: true });

  s.addNotes(fs.readFileSync(process.argv[2], "utf8"));
  await pres.writeFile({ fileName: process.argv[3] });
  console.log("wrote", process.argv[3]);
}

main().catch((err) => { console.error(err); process.exit(1); });
