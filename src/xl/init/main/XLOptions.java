package xl.init.main;
import java.io.File;
import org.kohsuke.args4j.Option;

public class XLOptions {

	

	@Option(name="-v", usage="version information")
	private boolean isVersion = false;
	
	@Option(name="-help", usage="Usage")
	private boolean help = false;
	
	@Option(name="-debug", usage="debug mode")
	private boolean debug = false;
		
	@Option(name="-g",usage="Group Code")
	private String grp_code = null;

	@Option(name="-p",usage="Policy Code")
	private String pol_code = null;

	@Option(name="-t",usage="Table")
	private String table = null;
	
	@Option(name="-bulk_mode",usage="Bulk_mode")
	private String bulk_mode = null;
	
	@Option(name="-commit_ct",usage="Commit_count")
	private Integer commitCount;
	
	@Option(name="-paral",usage="Parallel")
	private Integer parallel;
	
	@Option(name="-bs",usage="Batch_size")
	private Integer batch_size;
	
	@Option(name="-fs",usage="Fetch_size")
	private Integer fetch_size;
	
	// NR_TRANS_EVENT_LOG의 Sequence 지정시
	// 하나만 지정할 경우는 minSeq 에 셋팅
	private long minSeq = 0;
	private long maxSeq = 0;
	

	/*
	 * @Option(name="-o",usage="[stdout|catalog]") private String output = "stdout";
	 */
	
	public static void usage()
	{
		System.out.println("Usage : ./xl_init [-g grpCode] [-p polCode] [-t TableOwner.TableName] [-bulk_mode Y|N*] [-v] [-help]");
	}

	public boolean isVersion() {
		return isVersion;
	}

	public void setVersion(boolean _isVersion) {
		isVersion = _isVersion;
	}

	public String getGrp_code() {
		return grp_code;
	}

	public void setGrp_code(String _grp_code) {
		grp_code = _grp_code;
	}

	public String getPol_code() {
		return pol_code;
	}

	public void setPol_code(String _pol_code) {
		pol_code = _pol_code;
	}

	public boolean isHelp() {
		return help;
	}

	public void setHelp(boolean help) {
		this.help = help;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}
	
	public String getTable() {
		return table;
	}

	public void setTable(String _table) {
		this.table = table;
	}
	
	public String getBulk_mode() {
		return bulk_mode;
	}

	public void setBulk_mode(String _bulk_mode) {
		this.bulk_mode = bulk_mode;
	}
	
	public Integer getCommit_count() {
		return commitCount;
	}

	public void setCommit_count(int _commitCount) {
		this.commitCount = commitCount;
	}
	
	public Integer getParallel() {
		return parallel;
	}

	public void setParallel(int _parallel) {
		this.parallel = parallel;
	}
	
	public Integer getBatch_size() {
		return batch_size;
	}

	public void setBatch_size(int _batch_size) {
		this.batch_size = batch_size;
	}
	
	public Integer getFetch_size() {
		return fetch_size;
	}

	public void setFetch_size(int _fetch_size) {
		this.fetch_size = fetch_size;
	}
    
}
