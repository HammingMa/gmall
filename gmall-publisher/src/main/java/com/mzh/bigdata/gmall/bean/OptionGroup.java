package com.mzh.bigdata.gmall.bean;

import java.util.List;

public class OptionGroup {
    private String title;
    private List<Option> options;

    public OptionGroup(String title, List<Option> options) {
        this.title = title;
        this.options = options;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }

    @Override
    public String toString() {
        return "OptionGroup{" +
                "title='" + title + '\'' +
                ", options=" + options +
                '}';
    }
}
