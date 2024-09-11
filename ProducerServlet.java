

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import com.newrelic.api.agent.Trace;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class ProducerServlet
 */
@WebServlet("/ProducerServlet")
public class ProducerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private KafkaProducer<String, String> producer;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ProducerServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see Servlet#init(ServletConfig)
	 */
	public void init(ServletConfig config) throws ServletException {
		Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
	}

	/**
	 * @see Servlet#destroy()
	 */
	public void destroy() {
		producer.close();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String message = request.getParameter("message");
        if (message != null) {
            // Create the ProducerRecord with headers
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", null, message);

            // Add the custom header "foo=bar"
            Headers headers = record.headers();
            headers.add(new RecordHeader("foo", "bar".getBytes(StandardCharsets.UTF_8)));

            // Send the message to Kafka
            producer.send(record);

            response.getWriter().println("Message sent: " + message + " with header foo=bar");
        } else {
            response.getWriter().println("No message provided");
        }
	}

}
