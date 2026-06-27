import json,re,sys,yaml
run=sys.argv[1] if len(sys.argv)>1 else '/tmp/wt-blend/_e2e/run1'
twb='/tmp/ddmx-stage1/workbook-content.twb'
AGG=r'(?:(?:sum|avg|average|min|max|median|distinct count|count) of |(?:avg|sum|min|max|med|cnt|ctd)\.\s*|(?:second|minute|hour|day|week|month|quarter|year) of )?'
mmap=json.load(open(f'{run}/master-map.json'))
mcols=yaml.safe_load(open(f'{run}/master-cols.yaml'))['columns']
def norm(s): return re.sub(r'[^a-z0-9]','',s.lower())
def desuffix(name): return re.sub(r'_\d+$','',name)   # WEEK_OF_2 -> WEEK_OF
# index master cols by normalized de-suffixed warehouse base
by_base={}
for c in mcols:
    by_base.setdefault(norm(desuffix(c['name'])), c)   # first wins (un-suffixed preferred since order)
added=0
# (a) space/underscore/case-flexible entry for every master column name
for c in mcols:
    pat=re.escape(c['name']).replace('_',r'[\s_]*')
    key=f'(?i)^{AGG}{pat}$'
    if key not in mmap: mmap[key]={'id':c['id'],'name':c['name']}; added+=1
# (b) caption -> column via .twb <column caption=.. name=..>
xml=open(twb,encoding='utf-8',errors='replace').read()
caps=re.findall(r"<column\b[^>]*\bcaption='([^']+)'[^>]*\bname='\[([^']+)\]'", xml)
capmap=0
by_name={norm(c['name']):c for c in mcols}            # master col by normalized display name
namemap=0
for cap,field in caps:
    base=re.sub(r'\s*\([^)]*\)\s*','',field)          # strip (Custom SQL QueryN)
    base=re.sub(r'\s*\(copy\)_\d+$','',base)
    col=by_base.get(norm(base))
    if col:
        key=f'(?i)^{AGG}{re.escape(cap)}$'
        if key not in mmap: mmap[key]={'id':col['id'],'name':col['name']}; capmap+=1
    # (c) internal field NAME -> the master col matching its CAPTION. Chart shelves
    # reference the Tableau internal name ([Calculation_NNN], [X (copy)_NNN]); a
    # calc's master col is named by caption, so without this the shelf ref dangles
    # and the whole chart drops. Only map when the caption resolves to a master col.
    capcol=by_name.get(norm(cap))
    if capcol:
        nkey=f'(?i)^{AGG}{re.escape(field)}$'
        if nkey not in mmap: mmap[nkey]={'id':capcol['id'],'name':capcol['name']}; namemap+=1
json.dump(mmap,open(f'{run}/master-map.json','w'),indent=2)
print(f"enriched mmap: +{added} normalized, +{capmap} caption, +{namemap} internal-name entries (total {len(mmap)})")
