"""
Generates the shared audience network for a scan
"""

import pandas as pd
import itertools

df = pd.read_csv(
    "redtagging-merged-processed.csv",
    usecols=["linker_id","linker_slug","source_name","source_url"],
    dtype=str
)

# Determine number of agents per source
hubs = pd.DataFrame()
for i in df.index.tolist():
    source = df.at[i,'source_name']
    dest = df.at[i,'linker_slug']
    if source != dest:
        try:
            hubs.at[source,'connectors'] += 1
        except:
            hubs.at[source,'connectors'] = 1
hubs = hubs[ hubs['connectors'] > 20 ].sort_values(by='connectors',ascending=False).reset_index()
hubs = hubs[(~hubs['index'].isna()) & (hubs['index'] != 'bit.ly')]

def shared_audience(pages,posts,site_ind1,site_ind2,threshold=0):
    site1 = pages.loc[site_ind1,'index']
    site2 = pages.loc[site_ind2,'index']
    agents1 = set(list(posts[posts['source_name'] == site1]['linker_slug']))
    agents2 = set(list(posts[posts['source_name'] == site2]['linker_slug']))
    common_agents = agents1.intersection(agents2)

    if len(common_agents) >= threshold:
        return {"shared": True, "commons": len(common_agents) }
    else:
        return {"shared": False, "commons": len(common_agents) }


pages_df = hubs
num_pages = len(hubs)

links = []
commons = []
page_inds = pages_df.index.tolist()
pairs = list(itertools.combinations(page_inds,2))
counter = 0
for pair in pairs:
    total_pairs = num_pages*(num_pages-1)/2
    res = shared_audience(pages=pages_df,posts=df,site_ind1=pair[0],site_ind2=pair[1])
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

links_final = links[links['link'] > 1]
links_final.to_csv("karapatan_shared_audience.csv",index=False)