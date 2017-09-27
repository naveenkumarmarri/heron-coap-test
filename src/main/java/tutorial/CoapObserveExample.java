package tutorial;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapHandler;
import org.eclipse.californium.core.CoapObserveRelation;
import org.eclipse.californium.core.CoapResponse;
import org.eclipse.californium.core.coap.MediaTypeRegistry;

public class CoapObserveExample {

	public static void main(String[] args) {
		
		CoapClient client = new CoapClient("coap://californium.eclipse.org:5683/obs");

		System.out.println("SYNCHRONOUS");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		System.out.println("OBSERVE (press enter to exit)");
	
		CoapObserveRelation relation = client.observe(
				new CoapHandler() {
					@Override public void onLoad(CoapResponse response) {
						String content = response.getResponseText();
						System.out.println("NOTIFICATION: " + content);
					}
					
					@Override public void onError() {
						System.err.println("OBSERVING FAILED (press enter to exit)");
					}
				});
		
		// wait for user
		try { br.readLine(); } catch (IOException e) { }
		
		System.out.println("CANCELLATION");
		
		relation.proactiveCancel();
	}
}

