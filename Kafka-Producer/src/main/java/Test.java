import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Test {
    public static void main(String[] args) {
        System.out.println("Hello");
        String path1 = "/Users/utkarshnath/Desktop/consumer-reviews-of-amazon-products/Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products_May19.csv";

        try {
            BufferedReader br = new BufferedReader(new FileReader(path1));
            String head = br.readLine();
            System.out.println(head);
            String head1 = br.readLine();
            System.out.println(head1);
            String head2 = br.readLine();
            System.out.println(head2);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
