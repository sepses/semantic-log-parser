public class LogLine {
    public String LineId;
    public String EventMonth;
    public String EventDay;
    public String EventTime;
    public String Level;
    public String Component;
    public String Content;
    public String EventId;
    public String EventTemplate;
    public String ParameterList;

    public LogLine(String LineId, String EventMonth, String EventDay, String EventTime, String Level, String Component, String Content, String EventId, String EventTemplate, String ParameterList){
        this.LineId = LineId;
        this.EventMonth = EventMonth;
        this.EventDay = EventDay;
        this.EventTime = EventTime;
        this.Level = Level;
        this.Component = Component.replaceAll("[\\[\\]]", "");
        this.Content = Content;
        this.EventId = EventId;
        this.EventTemplate = EventTemplate;
        this.ParameterList = ParameterList;
    }
}