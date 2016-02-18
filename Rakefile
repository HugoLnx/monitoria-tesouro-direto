require './model'

desc "Get new values of public titles"
task "get" do
  data = Crawler.get_new_data
  if data.nil?
    Log.info "Data wasn't collected"
  else
    Persistence.save_data(data)
  end
end

desc "Get new values of public titles (force)"
task "get-force" do
  data = Crawler.get_new_data force: true
  if data.nil?
    Log.info "Data wasn't collected"
  else
    Persistence.save_data(data)
  end
end
