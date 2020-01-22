package kafka

import spock.lang.Specification

import java.util.concurrent.Semaphore

class EventHandlerTest extends Specification {





    def "test semaphore ticket count"() {

        given:
        def expected = "{\"date_time\":\"2014-08-11 08:22:12\",\"site_name\":\"2\",\"posa_continent\":\"3\",\"user_location_country\":\"2\",\"user_location_region\":\"348\",\"user_location_city\":\"48862\",\"orig_destination_distance\":\"\",\"user_id\":\"12\",\"is_mobile\":\"0\",\"is_package\":\"1\",\"channel\":\"9\",\"srch_ci\":\"2014-08-29\",\"srch_co\":\"2014-09-02\",\"srch_adults_cnt\":\"2\",\"srch_children_cnt\":\"0\",\"srch_rm_cnt\":\"1\",\"srch_destination_id\":\"8250\",\"srch_destination_type_id\":\"1\",\"is_booking\":\"1\",\"cnt\":\"1\",\"hotel_continent\":\"1\",\"hotel_country\":\"2\",\"hotel_market\":\"3\",\"hotel_cluster\":\"1\"}";

        def producer = Mock(org.apache.kafka.clients.producer.KafkaProducer)
        def ticket = 10
        def semaphore = new Semaphore(ticket)
        def handler = new EventHandler(expected, producer, "topic", semaphore)

        when:
        handler.run()

        then:
        semaphore.availablePermits() == ticket + 1
        noExceptionThrown()

    }



    def "error if json is damaged"() {

        given:
        def expected = "{damaged json}";

        def producer = Mock(org.apache.kafka.clients.producer.KafkaProducer)
        def semaphore = new Semaphore(0)

        def handler = new EventHandler(expected, producer, "topic", semaphore)

        when:
        handler.run()

        then:
        thrown(ArrayIndexOutOfBoundsException)

    }

}
