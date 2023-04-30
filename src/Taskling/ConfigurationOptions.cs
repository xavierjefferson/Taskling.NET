namespace Taskling;

public class ConfigurationOptions
{
    public string DB { get; set; }
    public int TO { get; set; } = 120;
    public bool E { get; set; } = true;
    public int CON { get; set; } = -1;
    public int KPLT { get; set; } = 14;
    public int KPDT { get; set; } = 40;
    public int MCI { get; set; } = 1;
    public bool KA { get; set; } = true;
    public double KAINT { get; set; } = 1;
    public double KADT { get; set; } = 10;
    public double TPDT { get; set; }
    public bool RPC_FAIL { get; set; } = false;
    public int RPC_FAIL_MTS { get; set; }
    public int RPC_FAIL_RTYL { get; set; }
    public bool RPC_DEAD { get; set; } = false;
    public int RPC_DEAD_MTS { get; set; }
    public int RPC_DEAD_RTYL { get; set; }
    public int MXBL { get; set; } = 10000;
    public int MXCOMP { get; set; } = 2000;
    public int MXRSN { get; set; } = 1000;
}