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

# NOTE (n4pi.4, 2026-06-27): the converter now self-validates calc columns/metrics
# — it decodes XML entities, translates IN→or-chain, strips //comments, resolves
# calc-on-calc refs to captions, reconciles caption↔SQL-alias, routes untranslatable
# table-calc/LOD/param calcs to workbookPatterns, and drop-and-surfaces any calc with
# an unresolvable sibling ref (transitive). So the old aggressive harness prune (which
# required every ref to be [Custom SQL/<alias>] and therefore dropped ALL calc columns)
# is obsolete. We trust the converter output and just report its calc-column counts.
conv['model']['pages'].each do |pg|
  (pg['elements'] || []).each do |el|
    next unless el.dig('source', 'kind') == 'sql'
    alias_cols = (el['columns'] || []).count { |c| (c['formula'] || '') =~ %r{\A\[Custom SQL/[^\]]+\]\z} }
    calc_cols  = (el['columns'] || []).size - alias_cols
    puts "  converter element '#{el['name']}': #{alias_cols} alias + #{calc_cols} calc col(s), #{(el['metrics'] || []).size} metric(s)"
  end
end

FOLDER = '57e59735-86b9-40b0-b029-217205406f57'
conv['model']['folderId'] = FOLDER
File.write(File.join(WORK,'dm-spec.json'), JSON.pretty_generate(conv['model']))

puts "\n== 2. POST DM (with error-column repair loop) =="
# The converter self-validates calc refs, but Sigma's authoritative type checker
# may still reject a calc for a reason we can't predict offline (e.g. a boolean
# comparison form, an exotic function-arg shape). post-and-readback exits 2 and
# names those columns. Use Sigma as ground truth: read the error columns, prune
# them + any calc that transitively depends on them, delete the bad DM, re-post.
# This is the self-heal pattern the skill (n4pi.5) should own. Never silent —
# every pruned calc is printed.
$LOAD_PATH.unshift File.join(HERE, 'lib')
require 'sigma_rest'
def prune_error_cols!(spec, error_names)
  bad = error_names.map(&:downcase).to_set
  loop do
    grew = false
    spec['pages'].each do |pg|
      (pg['elements'] || []).each do |el|
        next unless el.dig('source', 'kind') == 'sql'
        %w[columns metrics].each do |k|
          (el[k] || []).each do |c|
            nm = (c['name'] || '').downcase
            next if nm.empty? || bad.include?(nm)
            refs = (c['formula'] || '').scan(/\[([^\]\[\/]+)\]/).flatten.map(&:downcase)
            if refs.any? { |r| bad.include?(r) } && !(c['formula'] =~ %r{\A\[Custom SQL/})
              bad << nm; grew = true
            end
          end
        end
      end
    end
    break unless grew
  end
  spec['pages'].each do |pg|
    (pg['elements'] || []).each do |el|
      next unless el.dig('source', 'kind') == 'sql'
      keepids = nil
      if el['columns']
        before = el['columns'].size
        el['columns'] = el['columns'].reject { |c| bad.include?((c['name'] || '').downcase) }
        keepids = el['columns'].map { |c| c['id'] }.to_set
        el['order'] = (el['order'] || []).select { |i| keepids.include?(i) } if el['order']
      end
      el['metrics'] = el['metrics'].reject { |c| bad.include?((c['name'] || '').downcase) } if el['metrics']
    end
  end
  bad
end

dm = nil
3.times do |attempt|
  _o, _e, st = run!(['ruby', File.join(HERE,'post-and-readback.rb'), '--type','datamodel','--spec',File.join(WORK,'dm-spec.json'),'--out',File.join(WORK,'dm-ids.json'),'--workdir',WORK], allow_fail: true)
  dm = JSON.parse(File.read(File.join(WORK,'dm-ids.json'))) rescue nil
  break if st.success?
  unless dm && dm['dataModelId']
    abort "DM post failed and no dataModelId written — cannot repair"
  end
  cols = Sigma.request(:get, "/v2/dataModels/#{dm['dataModelId']}/columns")
  errs = (cols['entries'] || []).select { |c| c.dig('type','type') == 'error' }.map { |c| c['label'] }
  if errs.empty?
    abort "post-and-readback failed (exit #{st.exitstatus}) but no error-type columns found — different failure"
  end
  puts "  repair attempt #{attempt+1}: Sigma flagged #{errs.size} error column(s): #{errs.join(', ')}"
  spec = JSON.parse(File.read(File.join(WORK,'dm-spec.json')))
  pruned = prune_error_cols!(spec, errs)
  puts "  pruned #{pruned.size} column(s)/metric(s) (incl. transitive dependents); deleting bad DM #{dm['dataModelId']} and re-posting"
  Sigma.request(:delete, "/v2/files/#{dm['dataModelId']}") rescue nil
  File.write(File.join(WORK,'dm-spec.json'), JSON.pretty_generate(spec))
end
abort "DM still failing after repair attempts" unless dm
# Re-sync the in-memory model with the (possibly repaired) posted spec, so master
# derivation below doesn't emit master columns for fields the repair loop pruned
# (which would 400 the workbook POST with "Dependency not found").
conv['model'] = JSON.parse(File.read(File.join(WORK,'dm-spec.json')))
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

# Enrich the master-map with space/underscore-flexible + Tableau-caption entries
# (n4pi.7) so chart refs that use friendly captions resolve to warehouse-named cols.
run!(['python3', File.join(__dir__, 'enrich-mmap.py'), WORK])

puts "\n== 4. build-charts =="
layout = File.join(WORK,'layout.json')
bc = ['ruby', File.join(HERE,'build-charts-from-signals.rb'), '--tableau-dir',WORK,'--layout',layout,'--master-map',File.join(WORK,'master-map.json'),'--master-element-id','master','--page-per-dashboard','--out',File.join(WORK,'chart-specs.json'),'--coverage-out',File.join(WORK,'coverage.json')]
bc += ['--meta', File.join(WORK,'layout-meta.json')] if File.exist?(File.join(WORK,'layout-meta.json'))
bc += ['--auto-controls'] if File.exist?(File.join(WORK,'layout-meta.json'))
run!(bc, allow_fail: true)

puts "\n== 5. build-workbook-spec =="
run!(['ruby', File.join(HERE,'build-workbook-spec.rb'), '--chart-specs',File.join(WORK,'chart-specs.json'),'--dm-ids',File.join(WORK,'dm-ids.json'),'--master-cols',File.join(WORK,'master-cols.yaml'),'--workbook-name','DDMX Blend E2E (n4pi)','--folder-id',FOLDER,'--mode','dashboard','--dm-element-name',fact['name'],'--out',File.join(WORK,'wb-spec.json')], allow_fail: true)

# build-workbook-spec page-id slug can contain invalid chars (e.g. '+'); Sigma
# requires /^[a-zA-Z0-9_-]{1,64}$/. Sanitize before POST (skill bug, n4pi.5).
wbspec = JSON.parse(File.read(File.join(WORK,'wb-spec.json')))
(wbspec['pages'] || []).each { |pg| pg['id'] = pg['id'].gsub(/[^a-zA-Z0-9_-]/, '-')[0,64] if pg['id'] }

# Prune viz elements that reference master columns we don't have (charts plotting
# untranslated Tableau calcs/params — n4pi.4). Surface the count (never silent).
master_names = mcols.map { |c| c['name'].downcase }.to_set
ref_re = /\[master\/([^\]]+)\]/i
collect = lambda { |o, acc| case o
  when Hash  then o.each_value { |v| v.is_a?(String) ? acc.concat(v.scan(ref_re).flatten) : collect.call(v, acc) }
  when Array then o.each { |v| collect.call(v, acc) } end; acc }
dropped_viz = 0
(wbspec['pages'] || []).each do |pg|
  kept = (pg['elements'] || []).select do |e|
    miss = collect.call(e, []).reject { |r| master_names.include?(r.downcase) }
    if miss.any? then dropped_viz += 1; false else true end
  end
  pg['elements'] = kept
end
puts "viz prune: dropped #{dropped_viz} element(s) referencing untranslated calcs/params (n4pi.4)"
File.write(File.join(WORK,'wb-spec.json'), JSON.pretty_generate(wbspec))

puts "\n== 6. POST workbook =="
o,e,s = run!(['ruby', File.join(HERE,'post-and-readback.rb'), '--type','workbook','--spec',File.join(WORK,'wb-spec.json'),'--out',File.join(WORK,'wb-ids.json'),'--workdir',WORK], allow_fail: true)
puts "\n== POST workbook stderr tail (resolution errors) =="
puts (e||'')[-3000..] || e

puts "\n== 7. migration notes (Not Migrated and why) =="
# Turn every dropped tile into an actionable punch-list entry (n4pi.8 surfacing,
# bead ncwe) so no empty tab is mysterious: param measure-picker → control-driven
# Switch, field absent from source SQL, inert/commented source calc, window/LOD, etc.
run!(['node', File.join(__dir__, 'migration-notes.mjs'), WORK, TWB, CONN, DB, SCHEMA], allow_fail: true)
puts "→ #{File.join(WORK, 'migration-notes.md')}"
