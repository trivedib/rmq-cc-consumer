# frozen_string_literal: true

require_relative "consumer/version"
require 'bunny'

module Cc
  module Consumer
    class Error < StandardError; end
    # Your code goes here...
    connection = Bunny.new
    connection.start

    channel = connection.create_channel
    exchange = channel.topic('CC_DATA')
    queue = channel.queue("cc_data_queue", :durable => true, :auto_delete => false)
    queue.bind(exchange, :routing_key => 'CC_DATA')

    puts '[consumer] waiting for cc data. To exit press CTRL + C'

    begin
      # block: true is only used to keep the main thread alive.
      # avoid using it in real world app.
      queue.subscribe(:manual_ack => true, :block => true) do |delivery_info, _properties, body|
        puts "[producer] #{delivery_info.routing_key}:#{body}"
        channel.ack(delivery_info.delivery_tag) # this ack will ack the queue to remove the message
        puts "[consumer] sent ack about msg delivery: #{delivery_info.delivery_tag}"
      end
    rescue Interrupt => _
      channel.close
      connection.close

      exit(0)
    end
  end
end
