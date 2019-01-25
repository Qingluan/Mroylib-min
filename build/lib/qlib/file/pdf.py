from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from collections import Iterable

def output_to_pdf(IN, output, title=None, font=None,**kargs):

    c = canvas.Canvas(output)
    # c.setFont('song',10)
    text = c.beginText()

    text.setTextOrigin(1 * inch, 10 * inch)
    if font:
        c.setFont(font, 12)
    if title:
        c.drawString(1 *inch, 12 * inch, title)

    if isinstance(IN, str):
        text.textLines(IN.split("\n"))
    elif isinstance(IN, Iterable):
        for line in IN:
            print(line)
            text.textLine(line.strip())
    c.drawText(text)
    c.showPage()
    c.save()
    return output
            
