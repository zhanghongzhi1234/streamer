package com.bigdata.springboot.service;

import org.springframework.stereotype.Component;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

@Component
public class GaugeService {

    //Return Process CPU Load
    public double getProcessCpuLoad() {
        double dRet = 0;
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
            AttributeList list = mbs.getAttributes(name, new String[]{"ProcessCpuLoad"});
            if (list.isEmpty()) return Double.NaN;

            Attribute att = (Attribute) list.get(0);
            Double value = (Double) att.getValue();

            // usually takes a couple of seconds before we get real values
            if (value == -1.0)
                dRet = Double.NaN;
            // returns a percentage value with 1 decimal point precision
            dRet = ((int) (value * 1000) / 10.0);
        } catch (Exception ex){
            System.out.println(ex.toString());
        }

        return dRet;
    }

    //Returns the amount of used memory in bytes.
    public double getProcessMemory(){
        MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
        MemoryUsage memoryUsage = bean.getHeapMemoryUsage();
        return memoryUsage.getUsed();
    }
}
