package xl.init.poll;

import xl.init.conf.XLConf;

public class XLPollingEventQ
{

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
	
	synchronized public void waitEvent()
	{
		try
		{
			wait(XLConf.XL_MGR_POLLING_INT * 1000);
			
		}catch(Throwable e){ 
		}

	}

}
