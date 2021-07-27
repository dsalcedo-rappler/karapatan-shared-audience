import networkx as nx
import pandas as pd
from local_utils import download_from_gdrive

# Import Data
print("Downloading files...")
df = download_from_gdrive(
    "https://drive.google.com/file/d/1EyEv8sfnRw6bQtYpnYu6GO7c58PFHJWm/view?usp=sharing",
    colab_filename="shared_sources_raw.csv"
    )
df = df[["Post Created Date","linker_id","linker_slug","source_name","source_url"]]
df["created_year"] = df['Post Created Date'].apply(lambda x: str(x)[:4])
df["created_month"] = df['Post Created Date'].apply(lambda x: str(x)[:7])
df_full = df.copy()

# Filter data
filter_year = "2019"
df = df_full[ df_full['created_year'] == filter_year ]
df = df[~df['linker_slug'].isna()]

# Get roots
roots = pd.DataFrame()
for i in df.index.tolist():
    source = df.at[i,'source_name']
    dest = df.at[i,'linker_slug']
    if source != dest:
        try:
            roots.at[dest,'connectors'] += 1
        except:
            roots.at[dest,'connectors'] = 1
roots = roots[ roots['connectors'] > 1 ].sort_values(by='connectors',ascending=False).reset_index()
roots = roots[(~roots['index'].isna()) & (roots['index'] != 'bit.ly')]

# Get shared sources
def shared_sources(pages,posts,site_ind1,site_ind2,threshold=0):
    site1 = pages.loc[site_ind1,'index']
    site2 = pages.loc[site_ind2,'index']
    sources1 = set(list(posts[ posts['linker_slug'] == site1 ]['source_name']))
    sources2 = set(list(posts[ posts['linker_slug'] == site2 ]['source_name']))
    common_sources = sources1.intersection(sources2)

    if len(common_sources) >= threshold:
        return {"shared": True, "commons": len(common_sources) }
    else:
        return {"shared": False, "commons": len(common_sources) }

import itertools
pages_df = roots
num_pages = len(roots)

links = []
commons = []
page_inds = pages_df.index.tolist()
pairs = list(itertools.combinations(page_inds,2))
counter = 0
for pair in pairs:
    total_pairs = num_pages*(num_pages-1)/2
    res = shared_sources(pages=pages_df,posts=df,site_ind1=pair[0],site_ind2=pair[1])
    link = res['shared']
    commons.append(res['commons'])
    if link == True:
        links.append({
            "site1": pages_df.loc[pair[0],'index'],
            "site2": pages_df.loc[pair[1],'index'],
            "link": res['commons']
        })
    counter+= 1
    if counter%1000 == 0:
        print(f"Processed {counter} of {total_pairs} pairs")
    if counter == total_pairs:
        print(f"Processed {counter} of {total_pairs} pairs")

links = pd.DataFrame(links)
links_final = links[links['link'] > 3]

# Get communities
G = nx.Graph()
for i in links_final.index.tolist():
    source = links_final.at[i,'site1']
    dest = links_final.at[i,'site2']
    edge_size = links_final.at[i,'link']
    G.add_edge(source,dest, weight=edge_size)

from networkx import edge_betweenness_centrality as betweenness
def most_central_edge(G):
    centrality = betweenness(G, weight="weight")
    return max(centrality, key=centrality.get)

comp = nx.algorithms.community.centrality.girvan_newman(G)
communities = tuple(sorted(c) for c in next(comp))

for i in range(len(communities)):
    for node in communities[i]:
        G.nodes[node]['name'] = node
        G.nodes[node]['comm'] = i
roots = roots.set_index('index')

for root in roots.index.tolist():
    try:
        roots.at[root,'community'] = G.nodes[root]['comm']
    except:
        roots.at[root,'community'] = 99
roots = roots.reset_index()

# Export to csv
roots.to_csv(f"shared_sources_roots_{filter_year}.csv",index=False)
links_final.to_csv(f"shared_sources_{filter_year}.csv",index=False)

