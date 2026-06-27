require 'json'
wb = JSON.parse(File.read('_e2e/run1/wb-spec.json'))
master = wb['pages'].flat_map { |p| p['elements'] }.find { |e| e['kind']=='table' && e.dig('source','kind')=='data-model' }
master = master.dup; master['id']='master'
# control for Selection Param (Parameter 17)
ctl = { 'id'=>'el-ctl-sel', 'kind'=>'control', 'controlId'=>'ctl-parameter-17',
        'name'=>'Selection Param', 'controlType'=>'segmented',
        'source'=>{ 'kind'=>'manual','valueType'=>'text','values'=>['Signs','TAM','TAM per Sign'],'labels'=>[] },
        'value'=>'Signs' }
sw = 'Switch([ctl-parameter-17], "Signs", CountDistinct([Master/STORE_ACCOUNT_ID]), "TAM", Sum([Master/INCREMENTAL_SIGN_TAM]), "TAM per Sign", Sum([Master/INCREMENTAL_SIGN_TAM])/CountDistinct([Master/STORE_ACCOUNT_ID]))'
kpi = { 'id'=>'el-kpi-picker','kind'=>'kpi-chart','name'=>'Selection (param-driven)',
        'source'=>{ 'kind'=>'table','elementId'=>'master' },
        'columns'=>[ { 'id'=>'k-picker','name'=>'Selection Metric','formula'=>sw,
                       'format'=>{ 'kind'=>'number','formatString'=>',.0f' } } ],
        'value'=>{ 'columnId'=>'k-picker' } }
out = { 'name'=>'n4pi.8 Measure-Picker Proof','folderId'=>'57e59735-86b9-40b0-b029-217205406f57',
        'pages'=>[ { 'id'=>'page-proof','name'=>'Measure Picker','elements'=>[master, ctl, kpi] } ] }
File.write('_e2e/proof-wb.json', JSON.pretty_generate(out))
puts "wrote proof-wb.json (master + control + KPI Switch)"
