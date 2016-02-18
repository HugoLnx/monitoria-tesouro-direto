require './model'

def get(force=false)
  data = Crawler.get_new_data(force: force)
  if data.nil?
    Log.info "Data wasn't collected"
  else
    Persistence.save_data(data)
  end
rescue Exception => e
  Log.error(e)
end

desc "Get new values of public titles"
task "get" do
  get
end

desc "Get new values of public titles (force)"
task "get-force" do
  get true
end
