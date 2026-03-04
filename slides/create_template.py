#!/usr/bin/env python3
"""
Create a reusable PowerPoint template with custom layouts
"""

from pptx import Presentation
from pptx.util import Inches
from pptx.dml.color import RGBColor

# Color palette
BARCLAYS_BLUE = RGBColor(0, 133, 202)
DARK_BLUE = RGBColor(0, 73, 144)
WHITE = RGBColor(255, 255, 255)
LIGHT_GRAY = RGBColor(242, 242, 242)

def create_template():
    """Create a PowerPoint template with custom master slides"""
    prs = Presentation()
    prs.slide_width = Inches(10)
    prs.slide_height = Inches(7.5)

    # Save as template
    template_path = "slides/dtrack_template.pptx"
    prs.save(template_path)
    print(f"✅ Template created: {template_path}")
    return template_path

if __name__ == "__main__":
    create_template()
