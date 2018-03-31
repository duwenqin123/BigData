/**
 * Created by duwenqin123 on 9/27/17.
 */

public class LogAnalyze
{

    public static void main(String[] args)
            throws Exception
    {

        if(StatusCode.main(args) == 0)
            System.out.println("Task 1 succeed!");
        else
            System.out.println("Task 1 fail!");

        if(IpFrequency.main(args) == 0)
            System.out.println("Task 2 succeed!");
        else
            System.out.println("Task 2 fail!");

        if(UrlFrequency.main(args) == 0)
            System.out.println("Task 3 succeed!");
        else
            System.out.println("Task 3 fail!");

        if(UrlAccessAvg.main(args) == 0)
            System.out.println("Task 4 succeed!");
        else
            System.out.println("Task 4 fail!");


        System.exit(0);

    }

}

