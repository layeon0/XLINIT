package xl.init.util;

import xl.init.logger.XLLogger;

public class XLException
{
	public XLException()
	{
	}

	public static void outputExceptionLog( Exception e ) 
	{
		try{
			StringBuffer msg = new StringBuffer();

			// exception msg
			msg.append( e.toString() + "\n" ); 

			// trace msg 
            StackTraceElement[] se = e.getStackTrace();
            for (int i=0; i< se.length; i++) {
				msg.append( se[i].toString() );
				msg.append( "\n" );
            }   
			XLLogger.outputInfoLog( msg.toString() );

		} catch (Exception e1)  {
			XLLogger.outputInfoLog(e.toString());
		}

	}

    public static void outputExceptionLog( Throwable e )
    {
        try{
            StringBuffer msg = new StringBuffer();

            // exception msg
            msg.append( e.toString() + "\n" );

            // trace msg
            StackTraceElement[] se = e.getStackTrace();
            for (int i=0; i< se.length; i++) {
                msg.append( se[i].toString() );
                msg.append( "\n" );
            }
            XLLogger.outputInfoLog( msg.toString() );

        } catch (Exception e1)  {
        	XLLogger.outputInfoLog(e.toString());
        }

    }
	public static String getExceptionLog( Exception e ) 
	{
		try{
			StringBuffer msg = new StringBuffer();

			// exception msg
			msg.append( e.toString() + "\n" ); 
			// trace msg 
            StackTraceElement[] se = e.getStackTrace();
            for (int i=0; i< se.length; i++) {
				msg.append( se[i].toString() );
				msg.append( "\n" );
            }   
			return msg.toString();

		} catch (Exception e1)  {
			e1.printStackTrace(); // TEST CODE
			
			return  e.toString();
		}

	}

}

