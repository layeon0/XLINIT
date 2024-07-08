package xl.init.info;

import java.util.*;

import xl.init.logger.XLLogger;

public class XLDataQ
{
	private LinkedList<Vector<ArrayList<String>>> dataQList = new LinkedList<Vector<ArrayList<String>>>();
	//2010-01-07 modify for queue wake
//	private	final int	WAIT_TIMEOUT = 60000*60;  // 60 min
	private	final int	WAIT_TIMEOUT = 10000;  // 10 sec
	
	// cksohn - BULK mode oracle sqlldr - Ãß°¡
	synchronized public boolean notifyEvent()
	{
		
		try
		{
			notifyAll();
			return true;
		}catch(Exception e) { 
			return false;
		}

	}

	synchronized public boolean addDataQ(Vector<ArrayList<String>> _object)
	{
		
		// XLLogger.outputInfoLog("CKSOHN DEBUG--------------- addQ = " + _object.size());

		dataQList.addLast( _object );
		try
		{
			notifyAll();
			return true;
		}catch(Exception e) { 
			return false;
		}

	}

    synchronized public void removeDataQ()
    {
		try {
			dataQList.removeFirst();
		} catch (Exception e) {
		}
    }


    synchronized public Vector<ArrayList<String>> getDataQ()
    {
        try
        {
            return (Vector<ArrayList<String>>)dataQList.getFirst();


        }catch(Exception e) { 
			return null;
		}
    }
	
	synchronized public void waitDataQ()
	{
		try
		{
			wait(WAIT_TIMEOUT);
		}catch(Throwable e){ 
		}

	}

	synchronized public boolean isEmpty()
	{
		try {
			if ( dataQList.size() <= 0)  {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			return false;
		}

	}

    synchronized public int size()
    {
        try {
            return dataQList.size();
        } catch (Exception e) {
            return 0;
        }

    }

}
