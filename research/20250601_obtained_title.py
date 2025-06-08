# %%
import bs4
import requests
import start

# %%
# tesis de grafo
soup = bs4.BeautifulSoup(
    requests.get(
        "https://www.colibri.udelar.edu.uy/jspui/handle/20.500.12008/33158"
    ).text,
    features="html.parser",
)

# %%
title_row = soup.find(
    "td", class_="metadataFieldLabel", string=lambda text: "obtenido" in text.lower()
)
title_row.parent
# %%
titles = [
    str(t)
    for t in title_row.parent.find_all("td")[1].contents
    if type(t) != bs4.element.Tag
]
titles

# %%
# publicacion cientifica
soup_pub = bs4.BeautifulSoup(
    requests.get(
        "https://www.colibri.udelar.edu.uy/jspui/handle/20.500.12008/48644"
    ).text,
    features="html.parser",
)
# %%
title_row = soup_pub.find(
    "td", class_="metadataFieldLabel", string=lambda text: "obtenido" in text.lower()
)
title_row is None

# %%
