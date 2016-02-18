require 'bundler/setup'
require 'typhoeus'
require 'nokogiri'
require 'redis'
require 'logger'
require 'json'

module Log
  extend self
  LOGGER = Logger.new(ENV['LOGPATH'] || "./job.log")

  def info(msg)
    LOGGER.info msg
  end

  def error(msg)
    LOGGER.error msg
  end
end

module RedisFactory
  extend self

  def get
    Redis.new
  end
end

module Crawler
  extend self

  URI = "http://www.tesouro.fazenda.gov.br/tesouro-direto-precos-e-taxas-dos-titulos"

  def get_new_data(force: false)
    response = Typhoeus.get URI
    if response.success?
      return extract_data_from(response.body, force)
    else
      Log.error("Error requesting #{URI}. [#{response.code}]\n#{response.body}")
      return nil
    end
  end

  def extract_data_from(html, force)
    last_updated_key = "tesouro-direto-monitor|crawler|last-updated"
    doc = Nokogiri::HTML(html)
    redis = RedisFactory.get

    last_updated = redis.get last_updated_key
    last_updated = (!last_updated || last_updated.empty?) ? nil : DateTime.parse(last_updated)

    updated_date = extract_updated_date_from(doc)
    updated_date = (updated_date.to_time + 1).to_datetime if force # add one second to make different value on redis

    if force || !last_updated || last_updated < updated_date
      values = extract_public_titles_values_from doc
      redis.set last_updated_key, updated_date.iso8601
      {
        date: updated_date,
        values: values
      }
    else
      nil
    end
  end

  def extract_updated_date_from(doc)
    date_str = doc.css("b").map(&:content).map(&:strip).find{|content| content.match %r(\d{2}/\d{2}/\d{4} \d{2}:\d{2})}
    DateTime.parse(date_str, "%d/%m/%Y %H:%M")
  end
  
  def extract_public_titles_values_from(doc)
    table_rows = doc.css("tr.camposTesouroDireto")

    all_info = table_rows.map do |row|
      table_cells = row.css("td").map(&:content).map(&:strip)

      {
        type: table_cells[0].split(/[()]/)[1],
        expiration: extract_date(table_cells[1]),
        buy_tax: extract_tax(table_cells[2]),
        sell_tax: extract_tax(table_cells[3]),
        buy_price: extract_price(table_cells[4]),
        sell_price: extract_price(table_cells[5])
      }
    end
  end

  def extract_date(date_str)
    DateTime.parse(date_str, "%d/%m/%Y").to_time.utc
  end

  def extract_tax(tax_str)
    if tax_str == "-"
      nil
    else
      tax_str.gsub(",", ".").to_f
    end
  end

  def extract_price(price_str)
    price = price_str[2..-1].gsub(",", ".").to_f
    if price.zero?
      nil
    else
      price
    end
  end
end

module Persistence
  extend self

  def save_data(data)
    redis = RedisFactory.get

    updated_date = data[:date]
    values = data[:values]

    values.each do |public_title|
      key = key_to(public_title)
      is_persistent_attribute = -> (key,_) {%i{buy_tax sell_tax buy_price sell_price}.include? key}
      json = JSON.dump(public_title.keep_if(&is_persistent_attribute).merge(updated_date: updated_date))
      redis.zadd(key, updated_date.to_time.to_i, json)
      redis.zremrangebyscore(key, 0, (updated_date-35).to_time.to_i) # remove olds
    end
  end

  def key_to(public_title)
    [
      "tesouro-direto-monitor",
      public_title[:type],
      public_title[:expiration].to_s[0..9]
    ].join("|")
  end
end
