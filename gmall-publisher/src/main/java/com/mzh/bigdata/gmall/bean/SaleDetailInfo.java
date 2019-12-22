package com.mzh.bigdata.gmall.bean;

import java.util.List;
import java.util.Map;

public class SaleDetailInfo {

    private int Total;
    private List<OptionGroup> stat;
    private List<Map> detail;

    public SaleDetailInfo(int total, List<OptionGroup> stat, List<Map> detail) {
        Total = total;
        this.stat = stat;
        this.detail = detail;
    }

    public int getTotal() {
        return Total;
    }

    public void setTotal(int total) {
        Total = total;
    }

    public List<OptionGroup> getStat() {
        return stat;
    }

    public void setStat(List<OptionGroup> stat) {
        this.stat = stat;
    }

    public List<Map> getDetail() {
        return detail;
    }

    public void setDetail(List<Map> detail) {
        this.detail = detail;
    }


    @Override
    public String toString() {
        return "SaleDetailInfo{" +
                "Total=" + Total +
                ", stat=" + stat +
                ", detail=" + detail +
                '}';
    }
}
