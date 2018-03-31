package example;

/**
 * Created by duwenqin123 on 5/12/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.FileWriter;
import java.io.IOException;

public class ScanTable {

    public static void main(String[] args)
            throws IOException, InterruptedException
    {
        String tableName = "Wuxia";
        Configuration HBASE_CONFIG = HBaseConfiguration.create();

        Scan scan = new Scan();
        HTable table =new HTable(HBASE_CONFIG, tableName);
        ResultScanner resultScanner = table.getScanner(scan);

        for(Result r:resultScanner) {
            String word = Bytes.toString(r.getRow());
            byte[] valueBytes = r.getValue(Bytes.toBytes("datas"),
                    Bytes.toBytes("average"));
            String value = Bytes.toString(valueBytes);
            String fileName = "Wuxia.txt";
            FileWriter writer = new FileWriter(fileName, true);
            writer.write(word + "\t" + value+"\n");
            writer.close();
        }
    }
}

