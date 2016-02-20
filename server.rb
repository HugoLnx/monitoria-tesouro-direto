require 'sinatra'
require 'redis'
require 'json'
require 'date'

module RedisFactory
  def self.get
    Redis.new host: "104.236.62.220"
  end
end

class Server < Sinatra::Base
  REDIS = RedisFactory.get
  ATTRS = %w{buy_tax sell_tax buy_price sell_price}

  def redis
    REDIS
  end

  def parsedate(date_str)
    DateTime.parse(date_str).to_time.utc
  end

  def get_public_title_data(key)
    all_values = redis.zrange(key, 0, -1).map{|json| JSON.parse json}

    data = {}
    ATTRS.each do |attr|
      data[attr] = {
        now: all_values.last[attr]
      }
    end
    all_values.each do |values|
      ATTRS.each do |attr|
        data[attr][:max] = values[attr] if values[attr] && (data[attr][:max].nil? || data[attr][:max] < values[attr])
        data[attr][:min] = values[attr] if values[attr] && (data[attr][:min].nil? || data[attr][:min] > values[attr])
      end
    end
    data
  end

  set :views, settings.root + '/view'
  set :public_folder, File.dirname(__FILE__) + '/public'

  get '/public-titles.json' do
    public_titles = redis.smembers("tesouro-direto-monitor:public-titles")
    titles_data = {}
    public_titles.each do |key|
      _,type,date_str = key.split "|"
      expiration_date = parsedate date_str

      titles_data[type] ||= {}
      titles_data[type][expiration_date] = get_public_title_data key
    end

    JSON.dump(
      titulos_publicos: titles_data,
      ultima_atualizacao: redis.get('tesouro-direto-monitor|crawler|last-updated')
    )
  end

end
