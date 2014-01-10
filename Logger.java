import java.util.Calendar;

public class Logger
{
        public static void main(String[] args) 
        {
                String myMsg = args[0];

		printMsg(myMsg);

	}

	public static void printMsg(String myMsg) 
	{
		Calendar now = Calendar.getInstance();
			
		System.out.println(now.getTime() + ";" + myMsg);

	}

}
