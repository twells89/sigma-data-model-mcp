#!/usr/bin/env ruby
# synth-twb-e2e (n4pi.5 prototype): drive the mechanical Tableau→Sigma chain
# against a synthetic/empty warehouse — NO live Tableau. Verifies that the
# blend-collapsed DM lets workbook charts RESOLVE at live POST.
require 'json'; require 'fileutils'; require 'open3'; require 'set'
HERE = File.expand_path('~/sigma-migration-skills/plugins/tableau-to-sigma/skills/tableau-to-sigma/scripts')
$LOAD_PATH.unshift HERE
require File.join(HERE, 'mechanical-specs.rb')

TWB   = '/tmp/ddmx-stage1/workbook-content.twb'
BUILD = '/tmp/wt-blend/build/tableau.js'
CONN  = 'cb2f5180-641f-47bd-8efa-da9d590d855a'
DB, SCHEMA = 'REDACTED_DB', 'PUBLIC'
PRIOR = '/private/tmp/claude-502/-Users-tjwells/f0b792a2-7856-4423-8310-d6b44db22956/scratchpad/ddmx-e2e'
WORK  = '/tmp/wt-blend/_e2e/run1'

FileUtils.mkdir_p WORK
# Reuse the static .twb-derived discovery artifacts (unchanged .twb).
%w[get-workbook.json layout.json layout-meta.json workbook-content.twb].each do |f|
  FileUtils.cp(File.join(PRIOR, f), File.join(WORK, f)) if File.exist?(File.join(PRIOR, f))
end
FileUtils.cp_r(File.join(PRIOR, 'views'), WORK) unless File.exist?(File.join(WORK, 'views'))

def run!(cmd, **kw); puts "+ #{cmd.join(' ')}"; o,e,s = Open3.capture3(*cmd); puts o[-2000..] if o && !o.empty?; warn e[-2000..] if e && !e.empty?; abort "FAILED (#{s.exitstatus})" unless s.success? || kw[:allow_fail]; [o,e,s]; end

puts "\n== 1. converter (local build, collapse) =="
conv = MechanicalSpecs.run_converter(twb_path: TWB, conn: CONN, db: DB, schema: SCHEMA, mcp_build: BUILD, workdir: WORK)
puts "stats: #{conv['stats'].to_json}"
fx = MechanicalSpecs.fixup_dm_spec(conv['model'])
puts "fixup: fixed=#{fx[:fixed]} dropped=#{fx[:dropped].size}"

# Prune leaked Tableau calc columns/metrics (bug #3 / n4pi.4): keep only columns
# whose refs resolve to real [Custom SQL/<alias>] aliases; drop raw-Tableau
# leakage (&#10;, //, case/when, [Calculation_], Tableau IN, unknown refs).
conv['model']['pages'].each do |pg|
  (pg['elements'] || []).each do |el|
    next unless el.dig('source', 'kind') == 'sql'
    aliases = el['columns'].map { |c| c['formula'][%r{\[Custom SQL/(.+)\]$}, 1] }.compact.to_set
    col_ok = lambda do |f|
      return false if f.nil?
      return false if f =~ /&#10;|\/\/|\bcase\s+when\b|Calculation_/i
      f.scan(/\[([^\]\[]+)\]/).flatten.all? do |r|
        pre, _, nm = r.partition('/')
        !r.include?('/') ? false : (pre.strip.casecmp?('custom sql') && aliases.include?(nm))
      end
    end
    keep = el['columns'].select { |c| col_ok.call(c['formula']) }
    keepids = keep.map { |c| c['id'] }.to_set
    dropped_cols = el['columns'].size - keep.size
    el['columns'] = keep
    el['order'] = (el['order'] || []).select { |i| keepids.include?(i) }
    colnames = keep.map { |c| (c['name'] || '').downcase }.to_set
    met_ok = lambda do |m|
      f = m['formula'] || ''
      return false if f =~ /&#10;|\/\/|\bcase\s+when\b|\bend\b|Calculation_|\bin\s*\(/i
      f.scan(/\[([^\]\[]+)\]/).flatten.all? { |r| colnames.include?(r.downcase) }
    end
    dropped_mets = 0
    if el['metrics']
      before = el['metrics'].size
      el['metrics'] = el['metrics'].select { |m| met_ok.call(m) }
      dropped_mets = before - el['metrics'].size
    end
    puts "  prune: dropped #{dropped_cols} leaked col(s), #{dropped_mets} leaked metric(s); kept #{keep.size} col(s)" if dropped_cols + dropped_mets > 0
  end
end

FOLDER = '57e59735-86b9-40b0-b029-217205406f57'
conv['model']['folderId'] = FOLDER
File.write(File.join(WORK,'dm-spec.json'), JSON.pretty_generate(conv['model']))

puts "\n== 2. POST DM =="
run!(['ruby', File.join(HERE,'post-and-readback.rb'), '--type','datamodel','--spec',File.join(WORK,'dm-spec.json'),'--out',File.join(WORK,'dm-ids.json'),'--workdir',WORK])
dm = JSON.parse(File.read(File.join(WORK,'dm-ids.json')))
dm_id = dm['dataModelId']; dm_els = dm['pages'].flat_map { |p| p['elements'] }
dim_re = /(?i)\b(dim|date|calendar)\b/
fact = dm_els.reject { |e| e['name'] =~ dim_re }.max_by { |e| (e['columnLabels']||[]).size } || dm_els.max_by { |e| (e['columnLabels']||[]).size }
puts "DM #{dm_id}: #{dm_els.size} element(s); fact='#{fact['name']}' (#{(fact['columnLabels']||[]).size} labels)"

puts "\n== 3. derive_master =="
# pick_fact only recognizes warehouse-table/derived elements; a collapsed blend
# is a single kind:sql element, so select it directly (skill-side gap noted).
conv_fact = MechanicalSpecs.pick_fact(conv['model']) ||
            conv['model']['pages'].flat_map { |p| p['elements'] || [] }
                                  .select { |e| e.dig('source','kind') == 'sql' }
                                  .max_by { |e| (e['columns'] || []).size }
conv_base = MechanicalSpecs.base_of(conv['model'], conv_fact)
derived = MechanicalSpecs.derive_master(conv_fact, fact['name'], conv_base, fact['columnLabels'], conv['model'])
mcols = derived['master_columns']; mmap = derived['mmap']
File.write(File.join(WORK,'master-map.json'), JSON.pretty_generate(mmap))
require 'yaml'
File.write(File.join(WORK,'master-cols.yaml'), { 'columns' => mcols }.to_yaml)
puts "master: #{mcols.size} column(s)"

puts "\n== 4. build-charts =="
layout = File.join(WORK,'layout.json')
bc = ['ruby', File.join(HERE,'build-charts-from-signals.rb'), '--tableau-dir',WORK,'--layout',layout,'--master-map',File.join(WORK,'master-map.json'),'--master-element-id','master','--page-per-dashboard','--out',File.join(WORK,'chart-specs.json'),'--coverage-out',File.join(WORK,'coverage.json')]
bc += ['--meta', File.join(WORK,'layout-meta.json')] if File.exist?(File.join(WORK,'layout-meta.json'))
bc += ['--auto-controls'] if File.exist?(File.join(WORK,'layout-meta.json'))
run!(bc, allow_fail: true)

puts "\n== 5. build-workbook-spec =="
run!(['ruby', File.join(HERE,'build-workbook-spec.rb'), '--chart-specs',File.join(WORK,'chart-specs.json'),'--dm-ids',File.join(WORK,'dm-ids.json'),'--master-cols',File.join(WORK,'master-cols.yaml'),'--workbook-name','DDMX Blend E2E (n4pi)','--folder-id',FOLDER,'--mode','dashboard','--dm-element-name',fact['name'],'--out',File.join(WORK,'wb-spec.json')], allow_fail: true)

puts "\n== 6. POST workbook =="
o,e,s = run!(['ruby', File.join(HERE,'post-and-readback.rb'), '--type','workbook','--spec',File.join(WORK,'wb-spec.json'),'--out',File.join(WORK,'wb-ids.json'),'--workdir',WORK], allow_fail: true)
puts "\n== POST workbook stderr tail (resolution errors) =="
puts (e||'')[-3000..] || e
