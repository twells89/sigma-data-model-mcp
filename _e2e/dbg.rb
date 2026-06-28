require 'json'
HERE = File.expand_path('~/sigma-migration-skills/plugins/tableau-to-sigma/skills/tableau-to-sigma/scripts')
$LOAD_PATH.unshift HERE
require File.join(HERE, 'mechanical-specs.rb')
model = JSON.parse(File.read('/tmp/wt-blend/_e2e/run1/dm-spec.json'))
begin
  cf = MechanicalSpecs.pick_fact(model)
  puts "pick_fact: #{cf ? cf['name'].inspect : 'NIL'} (#{(cf['columns']||[]).size} cols)"
  cb = MechanicalSpecs.base_of(model, cf)
  puts "base_of: #{cb ? 'ok' : 'nil'}"
  d = MechanicalSpecs.derive_master(cf, cf['name'] || 'X', cb, nil, model)
  puts "derive_master OK: #{d['master_columns'].size} cols"
rescue => e
  puts "RAISED: #{e.class}: #{e.message}"
  puts e.backtrace.first(8)
end
