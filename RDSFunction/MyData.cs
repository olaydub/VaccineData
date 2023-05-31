public class MyData
{
    public DateData date { get; set; }
    public SiteData site { get; set; }
    public List<VaccineData> Vaccines { get; set; }
}

public class DateData
{
    public int month { get; set; }
    public int day { get; set; }
    public int year { get; set; }
}

public class SiteData
{
    public string id { get; set; }
    public string name { get; set; }
    public string zipCode { get; set; }
}

public class VaccineData
{
    public string brand { get; set; }
    public int total { get; set; }
    public int firstShot { get; set; }
    public int secondShot { get; set; }
}
