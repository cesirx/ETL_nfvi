"""
Script to get information from vSphere VMs and Hosts through vCenter API. (--help to get requested arguments)

Returns:
    - CSV file with the queried data.
    - HTML code with data structured in a sortable table

Tested in:
    vCenter 6.5u3

Requires (pip):
    pandas (for "daraframes")
    ninja2 (for "pandas.style")
    matplotlib (for "background_gradient")

By Cesar Ortega
"""

from pyVmomi import vim     # Module "pyVmomi" to connect to vSphere API
from pyVim.connect import SmartConnect, Disconnect
import ssl
import argparse
import getpass
import atexit   # Cleanup module
import time
import pandas as pd
import numpy as np
from VMdata import VMdata
from HostData import HostData
import os
#import datetime
import re
import requests
import json
import sys
from datetime import datetime, timezone

def parse_arguments():
    """Process input arguments."""

    parser = argparse.ArgumentParser()
    parser.add_argument('vcenter_ip', help='vCenter ip/fqdn')
    parser.add_argument('vcenter_user', help='vCenter login username')
    #parser.add_argument('-t', help='type of vCenter object which data is to be retrieved. Options are vm|host|cluster|datacenter', choices=['vm', 'host', 'cluster', 'datacenter'], required=True)
    parser.add_argument('-t', help='type of vCenter object which data is to be retrieved. Options are vm|host|cluster|datacenter', choices=['host', 'cluster', 'datacenter'], required=True)
    parser.add_argument('-n', help='name of vCenter object which data is to be retrieved', required=True)
    parser.add_argument('--gsw', help='connect to network Switches to retrieve port configuration values corresponding to host pNICs', action="store_true", required=False)
    #parser.add_argument('--esxi', help='connect to ESXi to retrieve enhanced configuration values corresponding to host pNICs', action="store_true", required=False)
    parser.add_argument('--esxiuser', help='Username to connect to ESXi and retrieve enhanced configuration values corresponding to host pNICs', required=False)
    parser.add_argument('--idracuser', help='Username to connect to iDRAC and retrieve enhanced configuration values corresponding to host pNICs', required=False)

    return parser.parse_args()

def connect(vcenter_ip, vcenter_user, vcenter_password):
    """Connect to vCenter and bypass SSL warnings.

    Parameters
    ----------
    vcenter_ip : string
        vCenter IP address or resolvable FQDN
    vcenter_user : string
        vCenter username (must have at least read privileges)
    vcenter_password : string
        vCenter password  

    Returns
    -------
    c
        vCenter Service Instance connection
    """

    s = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    s.verify_mode = ssl.CERT_NONE
    try:
        c = SmartConnect(host=vcenter_ip, user=vcenter_user, pwd=vcenter_password)
        #print ('Valid cert')
    except:
        c = SmartConnect(host=vcenter_ip, user=vcenter_user, pwd=vcenter_password, sslContext=s)
        #print ('Invalid or untrusted cert')
    return c

def get_obj(content, vimtype, name = None):
    """Return vCenter objects matching a given type.
    
    Parameters
    ----------
    content : pyVmomi.VmomiSupport.vim.ServiceInstanceContent
        connection to VMware vCenter
    vimtype : string
       pyvmomi object type to look for: VMs "[vim.VirtualMachine]", hosts "[vim.ComputeResource]", clusters "[vim.ClusterComputeResource]", etc. (per object name can be obtained from vCenter MOB URL)

    Results
    -------
    list
        list of vCenter object matching the given type
    """

    #content.rootFolder  --> starting point to look into
    #[vim.VirtualMachine]  -->   object types to look for
    #recursive  -->  whether we should look into it recursively
    return [item for item in content.viewManager.CreateContainerView(content.rootFolder, [vimtype], recursive=True).view]

def vm_scavenger(vm_obj):
    """Collect configuration data from a given VM.

    Parameters
    ----------
    vm_obj : pyVmomi.VmomiSupport.vim.VirtualMachine
       pyvmomi VM object 
       
       types: VMs "[vim.VirtualMachine]", hosts "[vim.ComputeResource]", clusters "[vim.ClusterComputeResource]", etc. (per object name can be obtained from vCenter MOB URL)

    Results
    -------
    df_v
        Dataframe with data about this VM. One counter per column
    """

    print('-- Gathering information from VM {}... '.format(vm_obj.name))
    df_v = pd.DataFrame(columns=['VM_Name', 'MOID', 'Host_Name', 'VM_vCPU', 'VM_vMEM_GB', 'VM_Provisioned_Storage_GB', 'VM_Space_In_Disk_GB',
                                    'VM_RealTime', 'VM_LatencySensitivity', 'VM_CoresPerSocket', 'VM_NUMA', 'CPU_Reservation_MHz', 'RAM_Reservation_GB',
                                    'SRIOV_vNICs', 'VMXNET3_vNICs', 'PCIPT_vNICs', 'VM_SwapFile_Size_GB', 'Cluster_Name', 'Datastore_Name', 'Datastore_Capacity_GB', 
                                    'Datastore_Free_GB', 'VM_Provisioned_vHDDs', 'VM_Snapshot', 'Restoration_Allowed', 'Snapshot_Allowed', 'VM_PowerState', 'VM_AntiAffinity', 
                                    'VM_Affinity', 'VM_AR_Rule_Compliant', 'VM_SP_Label', 'VM_SP_proxyURI', 'VM_SP_serviceURI', 'VM_SP_direction', 'Host_CPU_Package_MHz',
                                    'VirtualHardware_Version', 'UUID', 'Host_MOID', 'timestamp'])
    df_v_network = pd.DataFrame(columns=['VM_Name', 'vNIC_Name', 'vNIC_Type', 'vNIC_DPG', 'vNIC_VLANs', 'vNIC_MAC', 'pNIC_inUse', 'pNIC_inUse_NUMA', 'vNIC_GuestOS_Mapping_Order', 'vNIC_pciSlotNumber', \
                                            'vNIC_rxBuffer_Ring1_bytes', 'vNIC_rxBuffer_Ring1_fullTimes', 'vNIC_SRIOV_VF_ID', 'pNIC_PCI_Device', 'PortMirror_Session_Source', 'DPG_Active_Uplinks', 'DPG_Standby_Uplinks',\
                                             'DPG_Promiscuous_Mode', 'DPG_MAC_Address_Changes', 'DPG_Forged_Transmits', 'DPG_Load_Balancing', 'vNIC_dVS', 'dVS_LLDP', 'MOID', 'Host_Name', 'VM_NUMA', 'timestamp'])
    #df_v_network.astype=({'VM_Name':'str', 'vNIC_Name':'str', 'vNIC_Type':'str', 'vNIC_DPG':'str', 'vNIC_VLANs':'str', 'vNIC_MAC':'str', 'pNIC_inUse':'str', 'pNIC_inUse_NUMA':'str', 'vNIC_GuestOS_Mapping_Order':'str', 'vNIC_pciSlotNumber':'str', \
    #                                        'vNIC_rxBuffer_Ring1_bytes':'str', 'vNIC_rxBuffer_Ring1_fullTimes':'str', 'vNIC_SRIOV_VF_ID':'str', 'pNIC_PCI_Device':'str', 'DPG_Active_Uplinks':'str', 'DPG_Standby_Uplinks':'str',\
    #                                         'DPG_Promiscuous_Mode':'str', 'DPG_MAC_Address_Changes':'str', 'DPG_Forged_Transmits':'str', 'vNIC_dVS':'str', 'dVS_LLDP':'str', 'MOID':'str', 'Host_Name':'str', 'VM_NUMA':'str'})                         #dtype=[str,str,str,str,str,str,str,str,str,str,\
    

    vm_instance = VMdata(vm_obj)   # Creating an instance of VMdata class

    df_v = df_v.append({'VM_Name': vm_obj.name}, ignore_index=True)
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'MOID'] = vm_instance.vmMOID_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'timestamp'] = vm_instance.timestamp_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Host_Name'] = vm_instance.hostname_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Cluster_Name'] = vm_instance.clusterName_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Datastore_Name'] = vm_instance.dsName_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Datastore_Capacity_GB'] = vm_instance.dsCapacity_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Datastore_Free_GB'] = vm_instance.dsFree_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_Provisioned_vHDDs'] = vm_instance.hddNumber_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_Provisioned_Storage_GB'] = vm_instance.hddCapacity_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_SwapFile_Size_GB'] = vm_instance.swap_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_Space_In_Disk_GB'] = vm_instance.actualUsage_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_Snapshot'] = vm_instance.snapshot_calculator() 
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_PowerState'] = vm_instance.powerState_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_AntiAffinity'] = [vm_instance.antiAffinityRule_calculator()]   # Use [] to specify the list to be included in the cell
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_Affinity'] = [vm_instance.affinityRule_calculator()]   # Use [] to specify the list to be included in the cell
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_AR_Rule_Compliant'] = vm_instance.ruleCompliant_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_RealTime'] , df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_ResourcePool'] = vm_instance.realtime_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_LatencySensitivity'] = vm_instance.latency_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_CoresPerSocket'] = vm_instance.corePerSocket_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_vCPU'] = vm_instance.vCPU_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_vMEM_GB'] = vm_instance.vMEM_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_NUMA'] = vm_instance.numaNode_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_SP_Label'], df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_SP_proxyURI'], df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_SP_serviceURI'], df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VM_SP_direction'] = vm_instance.serialPort_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'CPU_Reservation_MHz'], df_v.at[(df_v['VM_Name'] == vm_obj.name), 'RAM_Reservation_GB'] = vm_instance.reservations_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Host_CPU_Package_MHz'] = vm_instance.hostPackageMHz_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'SRIOV_vNICs'] = vm_instance.sriovVirtualInterfaces_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VMXNET3_vNICs'] = vm_instance.vmxnet3VirtualInterfaces_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'PCIPT_vNICs'] = vm_instance.pciptVirtualInterfaces_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'VirtualHardware_Version'] = vm_instance.virtualHardwareVersion_calculator()
    df_v.at[(df_v['VM_Name'] == vm_obj.name), 'Host_MOID'] = vm_instance.hostMOID_calculator()
    #df_v.at[(df_v['VM_Name'] == vm_obj.name), 'UUID'] = vm_instance.UUID_calculator()

    #if vm_obj.runtime.powerState == 'poweredOn':
    for device in vm_obj.config.hardware.device:
        nic_type = vm_instance.get_vnic_type(device)
        if (nic_type != '') and ('DistributedVirtualPortBackingInfo' in str(type(device.backing))):
            vnic_name = device.deviceInfo.label
            if device.slotInfo:
                vnic_slotNumber = device.slotInfo.pciSlotNumber
            else:
                #vnic_slotNumber = np.nan
                vnic_slotNumber = ''
            vnic_dpg_name = vnic_dpg_promiscuous = vnic_dpg_macChange = vnic_dpg_forged = vnic_dpg_lb = vnic_dpg_vlans = vnic_dpg_active_uplinks = \
                                vnic_dpg_standby_uplinks = vnic_dvs_name = vnic_dvs_lldp = vnic_mac = vnic_sriov_vf = vnic_pciDevice = ""
            if "PCI-PT" not in nic_type:
                vnic_dpg_name = vm_instance.get_dpg_name(device.backing.port.portgroupKey)
                vnic_dpg_promiscuous, vnic_dpg_macChange,  vnic_dpg_forged = vm_instance.get_dpg_security(vnic_dpg_name)
                vnic_dpg_vlans = vm_instance.get_dpg_vlans(vnic_dpg_name)
                vnic_dvs_name = vm_instance.get_dvs_name(vnic_dpg_name)
                vnic_dvs_lldp = vm_instance.get_dvs_lldp(vnic_dpg_name)
                vnic_dpg_active_uplinks, vnic_dpg_standby_uplinks = vm_instance.get_dpg_active_uplinks(vnic_dpg_name, vnic_dvs_name)
                vnic_dpg_lb = vm_instance.get_dpg_lb_policy(vnic_dpg_name)
                vnic_mac = device.macAddress
                vnic_pmsession = vm_instance.get_vnic_pmSessions(device)

                pnic_numa = ''
                if "SR-IOV" in nic_type:
                    vnic_pciDevice = device.sriovBacking.physicalFunctionBacking.id
                    # SRIOV interfaces of powered-off VMs report no virtualFunctionBacking device
                    try:
                        vnic_sriov_vf = device.sriovBacking.virtualFunctionBacking.id
                    except:
                        vnic_sriov_vf = ''
                
                    if int(vnic_pciDevice.split(":")[1],16) > 130:
                        pnic_numa = '1'
                    else:
                        pnic_numa = '0'
            else:
                vnic_pciDevice = device.backing.id
                if int(vnic_pciDevice.split(":")[1],16) > 130:
                    pnic_numa = '1'
                else:
                    pnic_numa = '0'

            df_v_network = df_v_network.append({'VM_Name': vm_obj.name, 'MOID': vm_instance.vmMOID_calculator(), 'Host_Name': vm_obj.summary.runtime.host.name.split('.')[0], 'VM_NUMA': vm_instance.numaNode_calculator(), \
                                                'vNIC_Name': vnic_name, 'vNIC_Type': nic_type, 'vNIC_DPG': vnic_dpg_name,  'vNIC_VLANs': vnic_dpg_vlans, 'vNIC_MAC': vnic_mac, 'vNIC_dVS': vnic_dvs_name, \
                                                'vNIC_pciSlotNumber': vnic_slotNumber, 'vNIC_SRIOV_VF_ID': vnic_sriov_vf,'pNIC_PCI_Device': vnic_pciDevice, 'pNIC_inUse_NUMA': pnic_numa, 'vNIC_GuestOS_Mapping_Order': "", \
                                                'PortMirror_Session_Source': vnic_pmsession, 'DPG_Active_Uplinks': vnic_dpg_active_uplinks, 'DPG_Standby_Uplinks': vnic_dpg_standby_uplinks,  \
                                                'DPG_Promiscuous_Mode': vnic_dpg_promiscuous, 'DPG_MAC_Address_Changes': vnic_dpg_macChange, 'DPG_Forged_Transmits': vnic_dpg_forged, "DPG_Load_Balancing" : vnic_dpg_lb,
                                                'dVS_LLDP': vnic_dvs_lldp, 'timestamp': vm_instance.timestamp_calculator()}, ignore_index=True)

    df_v_network = vm_instance.pcislot_order(df_v_network)
    df_v_network.fillna('',inplace=True)

    return df_v, df_v_network

def host_scavenger(host_obj, arg_gsw, esxi_username='', esxi_password='', idrac_username='', idrac_password=''):
    """Collect info about VMs running in a given Host.

    Parameters
    ----------
    host_obj : pyVmomi.VmomiSupport.vim.ComputeResource
       pyvmomi Host object 

    Returns
    -------
    df_vms
        Dataframe with data from all VMs in this Host. One VM per row
    df_h
        Dataframe with data about this Host. One counter per column
    df_h_network
        Dataframe with Host pNIC information. One pNIC per row
    """

    df_vms = pd.DataFrame(columns=['VM_Name'])
    df_vms_network = pd.DataFrame(columns=['VM_Name'])
    df_h = pd.DataFrame(columns=['Host_Name', 'MOID', 'Cluster_Name', 'Provisioned_vCPUs', 'Provisioned_RAM', 'Datastore_Provisioned_GB', 'RealTime_vCPUs', 
                                'RealTime_Occupation_Perc', 'Total_CPU_Occupation_Perc', 'Total_RAM_Occupation_Perc', 'Max_RealTime_Occupation_Perc', 
                                'Max_OverProv_Ratio_Perc', 'Socket0_Pinned_vCPUs', 'Socket0_CPU_Occupation_Perc', 'Socket1_Pinned_vCPUs', 'Socket1_CPU_Occupation_Perc',
                                'Socket0_Pinned_vMEM', 'Socket0_RAM_Occupation_Perc', 'Socket1_Pinned_vMEM', 'Socket1_RAM_Occupation_Perc',
                                'SRIOV_VMs', 'SRIOV_VFs_Provisioned', 'PCIPT_VMs', 'PCIPT_Devices_Provisioned', 'Datastore_Name', 'Datastore_Capacity_GB', 
                                'Datastore_Free_GB', 'Datastore_ProvisionedSwap_GB', 'Datastore_MixedSpace_GB', 'ESXi_Version',
                                'ESXi_Build', 'BIOS_Version', 'CPLD_Version', 'iDRAC_Version', 'VIB_ISM_Version', 'ESXi_Rsv_Cores', 'ESXi_Rsv_RAM_GB', 'Model', 'timestamp'])
    df_h_network = pd.DataFrame(columns=['Host_Name', 'MOID', 'vmnic_Name', 'vmnic_Model', 'vmnic_Driver', 'vmnic_Driver_version', 'vmnic_Firmware_version', 'vmnic_MAC', 
                                'vmnic_Device', 'vmnic_Type', 'vmnic_Link_Status', 'vmnic_Configured_Speed_Mbps', 'vmnic_NUMA', 'vmnic_virtualSwitch', 'vmnic_configured_VFs', 'Host_calculated_VF_Vector', 
                                'Host_current_VF_Vector', 'Host_calculated_Trusted_Vector', 'Host_current_Trusted_Vector', 'iDRAC_NIC_Slot', 'iDRAC_EthernetPort_Slot', 
                                'physical_Switch_name', 'physical_Switch_port', 'physical_Switch_port_VLANs', 'vmnic_max_VFs', 'Model', 'Cluster_Name','timestamp'])
    
    print('** Gathering information from VMs in Host {}... '.format(host_obj.name.split('.')[0]))
    refreshDatastore(host_obj)
    for vm_obj in host_obj.vm:
        df_temp_v, df_temp_v_network = vm_scavenger(vm_obj)
        df_vms = df_vms.append(df_temp_v, ignore_index=True)   # Adding one by one each VM in the Host to the df_vms Dataframe
        df_vms_network = df_vms_network.append(df_temp_v_network, ignore_index=True)

    host_instance = HostData(host_obj, df_vms)   # Creating an instance of HostData class

    df_h = df_h.append({'Host_Name': host_obj.name.split('.')[0]}, ignore_index=True)
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'MOID'] = host_instance.hostMOID_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'timestamp'] = host_instance.timestamp_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Cluster_Name'] = host_instance.clustername_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'ESXi_Version'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'ESXi_Build'] = host_instance.esxiVersion()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'BIOS_Version'] = host_instance.biosVersion()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'ESXi_Rsv_Cores'] = host_instance.hypReservedCores_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'ESXi_Rsv_RAM_GB'] = host_instance.hypReservedMEM_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'RealTime_vCPUs'],  \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'RealTime_Occupation_Perc'] = host_instance.realtimevCPUs()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Max_RealTime_Occupation_Perc'] = host_instance.cpuRealTimeOccupationRatio()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Provisioned_vCPUs'], \
         df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Total_CPU_Occupation_Perc'] = host_instance.provisionedvCPUs()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Max_OverProv_Ratio_Perc'] = host_instance.cpuOccupationRatio()
    for socket in range(host_obj.hardware.numaInfo.numNodes):
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Socket'+str(socket)+'_Pinned_vCPUs'], \
            df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Socket'+str(socket)+'_CPU_Occupation_Perc'] = host_instance.socketProvisionedvCPUs(socket)
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Socket'+str(socket)+'_Pinned_vMEM'], \
            df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Socket'+str(socket)+'_RAM_Occupation_Perc'] = host_instance.socketProvisionedRAM(socket)
        #df_h['Host_Socket'+str(socket)+'_vCPUs'] = df_h['Host_Socket'+str(socket)+'_vCPUs'].astype(int)
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Model'] = host_instance.modelInfo_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Provisioned_RAM'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Total_RAM_Occupation_Perc'] = host_instance.provisionedRAM()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_Name'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_Capacity_GB'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_Free_GB'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_Provisioned_GB'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_ProvisionedSwap_GB'], \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'Datastore_MixedSpace_GB'] = host_instance.dsInfo_calculator()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'SRIOV_VMs'],  \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'SRIOV_VFs_Provisioned'] = host_instance.sriovVMs()
    df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'PCIPT_VMs'],  \
        df_h.at[(df_h['Host_Name'] == host_obj.name.split('.')[0]), 'PCIPT_Devices_Provisioned'] = host_instance.pciptVMs()

    # The following two metrics can only be obtained here as at this point df_vms and df_h are complete
    df_vms = host_instance.snapshotAllowed_calculator(df_h['Datastore_MixedSpace_GB'].item())
    df_vms = host_instance.restorationAllowed_calculator(df_h['Datastore_MixedSpace_GB'].item())

    df_h_network = host_instance.standardpNIC_info(df_h_network)
    df_h_network = host_instance.pciPassThroughNIC_info(df_h_network)
    df_h_network = host_instance.virtualSwitch_info(df_h_network)
    df_h_network = host_instance.pnicNuma_calculator(df_h_network)
    df_h_network = host_instance.pciDevice_Model(df_h_network)
    if esxi_username and esxi_password:
        df_h_network, df_h, df_vms_network = host_instance.connect_to_esxi(df_h_network, df_h, df_vms_network, esxi_username, esxi_password)
    df_h_network = host_instance.vectorVF_calculator(df_h_network)
    df_h_network.at[(df_h_network['Host_Name'] == host_obj.name.split('.')[0]), 'timestamp'] = host_instance.timestamp_calculator()
    df_h_network.at[(df_h_network['Host_Name'] == host_obj.name.split('.')[0]), 'Model'] = host_instance.modelInfo_calculator()
    df_h_network.at[(df_h_network['Host_Name'] == host_obj.name.split('.')[0]), 'Cluster_Name'] = host_instance.clustername_calculator()

    #if arg_gsw:
    #    df_h_network = host_instance.connect_to_GSW(df_h_network)

    if idrac_username and idrac_password:
        if 'R730' in df_h['Model'].item():  # Dell R730 iDRAC data takes too long to be retrieved via Redfish. It is retrieved faster through CGI.
            df_h, df_h_network = host_instance.idrac_cgi(df_h, df_h_network, idrac_username, idrac_password)
        elif 'PowerEdge' in df_h['Model'].item():
            #print("Connecting to {} iDRAC. Depending on host/iDRAC model this may take a while... be patient.\n".format(host_obj.name.split('.')[0]))
            df_h_network = host_instance.idrac_PCIeDeviceInfo(df_h_network, idrac_username, idrac_password)
            df_h_network = host_instance.idrac_ethernetInterfaces(df_h_network, idrac_username, idrac_password)
            df_h = host_instance.get_FW_inventory(df_h, idrac_username, idrac_password)
        else:
            # HP Blades code goes here
            pass
    
    return df_vms, df_vms_network, df_h, df_h_network

def cluster_scavenger(cluster_obj, arg_gsw, esxi_username='', esxi_password='', idrac_username='', idrac_password=''):
    """Iterate through Hosts in a given Cluster.

    Parameters
    ----------
    cluster_obj : pyVmomi.VmomiSupport.vim.ComputeResource
       pyvmomi Host object 

    Returns
    -------
    df_vms
        Dataframe with data from all VMs in this Cluster. One VM per row
    df_host
        Dataframe with data from all Hosts in this Cluster. One Host per row
    df_hosts_network
        Dataframe woth pNIC data from all Hosts in this Cluster. One pNIC per row
    df_c
        Dataframe with data about this Cluster. One counter per column
    """

    df_vms = pd.DataFrame(columns=['VM_Name'])
    df_vms_network = pd.DataFrame(columns=['VM_Name'])
    df_hosts = pd.DataFrame(columns=['Host_Name'])
    df_hosts_network = pd.DataFrame(columns=['Host_Name'])
    df_c = pd.DataFrame(columns=['Cluster_Name'])

    print('## Gathering information from Hosts in Cluster {}... '.format(cluster_obj.name))
    for host_obj in cluster_obj.host:
        df_temp_v, df_temp_v_network, df_temp_h, df_temp_h_network = host_scavenger(host_obj, arg_gsw, esxi_username, esxi_password, idrac_username, idrac_password)
        df_vms = df_vms.append(df_temp_v, ignore_index=True)   # Adding one by one the configuration data from each VM to the host Dataframe
        df_vms_network = df_vms_network.append(df_temp_v_network, ignore_index=True)
        df_hosts = df_hosts.append(df_temp_h, ignore_index=True)
        df_hosts_network = df_hosts_network.append(df_temp_h_network, ignore_index=True)
    
    df_c = df_c.append({'Cluster_Name': cluster_obj.name}, ignore_index=True)

    return df_vms, df_vms_network, df_hosts, df_hosts_network, df_c

def datacenter_scavenger(datacenter_obj, arg_gsw, esxi_username='', esxi_password='', idrac_username='', idrac_password=''):
    """Iterate through Clusters in a given Datacenter.

    Parameters
    ----------
    datacenter_obj : pyVmomi.VmomiSupport.vim.Datacenter
       pyvmomi Datacenter object 

    Returns
    -------
    df_vms
        Dataframe with data from all VMs in this Datacenter. One VM per row
    df_hosts
        Dataframe with data from all Hosts in this Datacenter. One Host per row
    df_clusters
        Dataframe with data from all Clusters in this Datacenter. One Host per row
    df_d
        Dataframe with data about this Datacenter. One counter per column
    """

    df_vms = pd.DataFrame(columns=['VM_Name'])
    df_vms_network = pd.DataFrame(columns=['VM_Name'])
    df_hosts = pd.DataFrame(columns=['Host_Name'])
    df_hosts_network = pd.DataFrame(columns=['Host_Name'])
    df_clusters = pd.DataFrame(columns=['Cluster_Name'])
    df_d = pd.DataFrame(columns=['Datacenter_Name'])

    #df_cluster = pd.DataFrame(columns=['VM_Name', 'Host_name', 'Cluster_name', 'Datastore_Name', 'Datastore_Capacity_GB', 'Datastore_Free_GB', 'VM_Provisioned_vHDDs', 
    #                                'VM_Provisioned_Storage_GB', 'VM_SwapFile_Size_GB', 'VM_Space_In_Disk_GB', 'VM_Snapshot', 'VM_PowerState', 'VM_AntiAffinity', 'VM_Affinity'])

    print('// Gathering information from Clusters in Datacenter {}... '.format(datacenter_obj.name))
    for cluster_obj in datacenter_obj.hostFolder.childEntity:
        df_temp_v, df_v_network, df_temp_h, df_temp_h_network, df_temp_c = cluster_scavenger(cluster_obj, arg_gsw, esxi_username, esxi_password, idrac_username, idrac_password)
        df_vms = df_vms.append(df_temp_v, ignore_index=True)   # Adding one by one the configuration data from each VM to the VM Dataframe
        df_vms_network = df_vms_network.append(df_v_network, ignore_index=True)
        df_hosts = df_hosts.append(df_temp_h, ignore_index=True)    # Adding one by one the configuration data from each Host to the host Dataframe
        df_clusters = df_clusters.append(df_temp_c, ignore_index=True)  # Adding one by one the configuration data from each Cluster to the Cluster Dataframe
        df_hosts_network = df_hosts_network.append(df_temp_h_network, ignore_index=True)

    df_d = df_d.append({'Datacenter_Name': datacenter_obj.name}, ignore_index=True)

    return df_vms, df_vms_network, df_hosts, df_hosts_network, df_clusters, df_d

def refreshDatastore(host_obj):
    """Refresh host Datastore Storage information."""

    for datastore_obj in host_obj.datastore:
        if datastore_obj.summary.type == "VMFS":
            datastore_obj.RefreshDatastoreStorageInfo() # Refresh Datastore capacity  

def findDatacenterObj(datacenter_string, content):
    """Get pyvmomi object corresponding to input Datacenter name.

    Parameters
    ----------
    datacenter_name : string
        vSphere Datacenter name to analyze
    content : pyVmomi.VmomiSupport.vim.ServiceInstanceContent
        connection to VMware vCenter

    Returns
    -------
    datacenter_obj
       pyvmomi Datacenter object 
    """

    datacenter = []
    for datacenter_obj in get_obj(content, vim.Datacenter):   # Create Datacenter view 
        if datacenter_string in datacenter_obj.name:  # Datacenter string found
            datacenter.append(datacenter_obj)

    if not datacenter:
        print()
        print("Cluster not found. Terminating program...")
        print()
        exit()

    return datacenter

def findClusterObj(cluster_name, content):
    """Get pyvmomi object corresponding to input Cluster name.

    Parameters
    ----------
    cluster_name : string
        vSphere Cluster to analyze
    content : pyVmomi.VmomiSupport.vim.ServiceInstanceContent
        connection to VMware vCenter

    Returns
    -------
    cluster_obj
       pyvmomi Cluster object 
    """

    cluster = ""
    for cluster_obj in get_obj(content, vim.ClusterComputeResource):   # Create Cluster view 
        if cluster_obj.name == cluster_name:  # Cluster found
            cluster = cluster_obj
            break

    if not cluster:
        print()
        print("Cluster not found. Terminating program...")
        print()
        exit()

    return cluster

def findHostObj(host_name, content):
    """Get pyvmomi object corresponding to input Host name.

    Parameters
    ----------
    host_name : string
        vSphere host to analyze
    content : pyVmomi.VmomiSupport.vim.ServiceInstanceContent
        connection to VMware vCenter

    Returns
    -------
    host_obj
       pyvmomi Host object 
    """

    host = ""
    for host_obj in get_obj(content, vim.HostSystem):   # Create Host view 
        if (host_obj.name.split('.')[0].lower() == host_name.lower()) or (host_obj.name.lower() == host_name.lower()):  # Host found
            host = host_obj
            break
    
    if not host:
        print()
        print("Host not found. Terminating program...")
        print()
        exit()

    return host

def findVMObj(vm_name, content):
    """Get pyvmomi object corresponding to input VM name.

    Parameters
    ----------
    vm_name : string
        vSphere VM to analyze
    content : pyVmomi.VmomiSupport.vim.ServiceInstanceContent
        connection to VMware vCenter

    Returns
    -------
    vm_obj
       pyvmomi VM object 
    """

    vm = ""
    for vm_obj in get_obj(content, vim.VirtualMachine):   # Create VM view 
        if vm_obj.name == vm_name:  # VM found
            vm = vm_obj
            break 

    if not vm:
        print()
        print("VM not found. Terminating program...")
        print()
        exit()

    return vm

def addStickyHeaderCSS(new_html):
    """Enhance HTML code with CSS to make table headers stick to the top upon scrolling.
    
    Parameters
    ----------
    new_html : string
        HTML code 
    Returns
    -------
    sticky_html
        Input HTML code with CSS code to make table headers sticky
    """
    
    # Add the above inside the <style></style>
    css_sticky_header_code = """
        th {
            position: -webkit-sticky;
            position: sticky;
            top: 0;
            z-index: 2;
            }
        """

    css_sticky_left_column_header_code = """
        th:first-child {
            position: -webkit-sticky;
            position: sticky;
            left: 0;
            z-index: 2;
            background: #ccc;
            }
        thead th:first-child {
            z-index: 5;
            }
    """

    css_sticky_left_column_code = """
        tbody tr td:nth-child(1) {  /*the first cell in each tr*/
		  position: -webkit-sticky;
		  position: sticky;
		  left: 0px;
		  z-index: 2
        }
    """

    #th_property = f'onclick="sortTable({i})"'
    sticky_html = re.sub(r'(</style>)',fr'{css_sticky_header_code}\n{css_sticky_left_column_header_code}\n{css_sticky_left_column_code}\n \1', new_html)

    return sticky_html

def tableColumnHideShow(html):
    """Add JQUERY code to hide table columns on checkbox click."""

    column_toggle_script = """
        <script> 
            $("input:checkbox").click(function(){
                var column = "." + $(this).attr("name");
                $(column).toggle();
            });
        </script> 
    """

    new_html = re.sub(r'(</style>)',fr'\1\n{column_toggle_script}\n', html)

    return new_html

def sliderCheckboxesHideShow(html, df):
    """Add one input checkbox per column and place them inside a JQUERY slider."""

    jquery_cdn = r'<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>'
    slide_header = r'<div id="flip" style="cursor: pointer; font-weight: bold;">Click to select table columns to display</div>'
    slider_script = """
        <script> 
            $(document).ready(function(){
                $("#flip").click(function(){
                    $("#panel").slideToggle("fast");
                });
            });
        </script>
    """
    # Columns will be hidden/shown by their class, so first we must ensure that every item in a column (TR) has the same class (not nececssary for the columns header)
    input_checkboxes = '<div id="panel">'

    for i in range(len(df.columns)):
        new_class_name = df.columns[i]
        html = re.sub(rf'"data row[0-9]* col{i}" ', r'"' + new_class_name + r'"', html) # Replace table cell class
        html = re.sub(rf'"col_heading level0 col{i}" ', r'"col_heading ' + new_class_name + r'"', html) # Replace table headers class
        input_checkboxes =  input_checkboxes + '\n' + f'<input name="{new_class_name}" type="checkbox" checked="checked" />{new_class_name}<br>'
    
    input_checkboxes =  input_checkboxes + '\n' + '</div>'

    slider_code = jquery_cdn + '\n' + slide_header + '\n' + slider_script + '\n' + input_checkboxes
    slider_css = """
        #panel, #flip {
            padding: 5px;
            text-align: left;
            background-color: #e5eecc;
            border: solid 1px #c3c3c3;
            width: 300px;
        }

        #panel {
            padding: 5px;
            display: none;
        }
    """
    
    new_html = re.sub(r'(</style>)',fr'{slider_css}\n\1\n{slider_code}\n', html)

    return new_html

def addSortFunctionJs(html, tableName):
    """Enhance HTML code with JavaScript function to sort tables when header is clicked.
    
    Parameters
    ----------
    html : string
        HTML code as returned by Pandas.Style.render()
    tableName : string
        HTML table name  
    Returns
    -------
    new_html
        Input HTML code with a sorting JS function and "onclick" data in each "th" Tag
    """

    # As we are using "format" to embed a variable in the JS code, the curly braces in that code must be doubled so that they are scaped.
    # Doubling up those curly braces escapes them; the final output will contain single { and } characters again
    javascript_sort_function = """
        <script>
            function sortTable(n) {{
                var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
                table = document.getElementById("T_{table}");
                switching = true;
                //Set the sorting direction to ascending:
                dir = "asc"; 
                /*Make a loop that will continue until
                no switching has been done:*/
                while (switching) {{
                    //start by saying: no switching is done:
                    switching = false;
                    rows = table.rows;
                    /*Loop through all table rows (except the
                    first, which contains table headers):*/
                    for (i = 1; i < (rows.length - 1); i++) {{
                        //start by saying there should be no switching:
                        shouldSwitch = false;
                        /*Get the two elements you want to compare,
                        one from current row and one from the next:*/
                        x = rows[i].getElementsByTagName("TD")[n];
                        y = rows[i + 1].getElementsByTagName("TD")[n];
                        var cmpX=isNaN(parseInt(x.innerHTML))?x.innerHTML.toLowerCase():parseInt(x.innerHTML);
                        var cmpY=isNaN(parseInt(y.innerHTML))?y.innerHTML.toLowerCase():parseInt(y.innerHTML);
                        cmpX=(cmpX=='-')?0:cmpX;
                        cmpY=(cmpY=='-')?0:cmpY;
                        /*check if the two rows should switch place,
                        based on the direction, asc or desc:*/
                        if (dir == "asc") {{
                            if (cmpX > cmpY) {{
                                //if so, mark as a switch and break the loop:
                                shouldSwitch= true;
                                break;
                            }}
                        }} else if (dir == "desc") {{
                            if (cmpX < cmpY) {{
                                //if so, mark as a switch and break the loop:
                                shouldSwitch = true;
                                break;
                            }}
                        }}
                    }}
                    if (shouldSwitch) {{
                        /*If a switch has been marked, make the switch
                        and mark that a switch has been done:*/
                        rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                        switching = true;
                        //Each time a switch is done, increase this count by 1:
                        switchcount ++;      
                    }} else {{
                        /*If no switching has been done AND the direction is "asc",
                        set the direction to "desc" and run the while loop again.*/
                        if (switchcount == 0 && dir == "asc") {{
                            dir = "desc";
                            switching = true;
                        }}
                    }}
                }}
            }}
        </script>
    """.format(table = tableName)

    html = html.replace('</th>','</th>\n')

    occurences = len(re.findall(r'(<th) (class="col)',html)) # Number of "<th class="col" strings in the HTML code
    # For every column/header in the table, replace "<th class=" with "<th onclick="sortTable(i)" class="
    for i in range(occurences):
        th_property = f'onclick="sortTable({i})"'
        html = re.sub(r'(<th) (class="col)',fr'\1 {th_property} \2', html, 1)

    new_html = html + '\n' + javascript_sort_function

    new_html = addStickyHeaderCSS(new_html)
    new_html = addColumnDraggable(new_html, tableName)
    new_html = addSeachField(new_html, tableName)

    return new_html

def addMultiSearch(new_html, tableName):
    """Add multisearch fields.
    
    Parameters
    ----------
    new_html : string
        HTML code
    tableName : string
        HTML table name
    Returns
    -------
    ms_table
        Input HTML code with JS script reference to make table columns draggable
    
    https://datatables.net/
    Example: http://live.datatables.net/giharaka/1/edit
    """

    javascript_multisearch_function = """
        <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>
        <link href="https://nightly.datatables.net/css/jquery.dataTables.css" rel="stylesheet" type="text/css" />
        <script src="https://nightly.datatables.net/js/jquery.dataTables.js"></script>

        <script>
            $(document).ready(function() {{
                // Setup - add a text input to each footer cell
                $('#T_{table} thead tr:eq(1) th').each( function () {{
                    var title = $(this).text();
                    $(this).html( '<input type="text" placeholder="Search '+title+'" class="column_search" />' );
                }} );
            
                // DataTable
                table = $('#T_{table}').DataTable({{
                    orderCellsTop: true,
                    initComplete: function () {{
						this.api().columns().every( function () {{
							var column = this;
							var select = $('<select><option value=""></option></select>')
								.appendTo( $(column.footer()).empty() )
								.on( 'change', function () {{
									var val = $.fn.dataTable.util.escapeRegex(
										$(this).val()
									);
			 
									column
										.search( val ? '^'+val+'$' : '', true, false )
										.draw();
								}} );
			 
							column.data().unique().sort().each( function ( d, j ) {{
								select.append( '<option value="'+d+'">'+d+'</option>' )
							}} );
						}} );
					}}
                }});
            
            // Apply the search
                $( '#T_{table} thead'  ).on( 'keyup', ".column_search",function () {{
            
                    table
                        .column( $(this).parent().index() )
                        .search( this.value )
                        .draw();
                }} );
            
            }} );
        </script>
    """.format(table = tableName)

    # Add Javasript code
    ms_table = re.sub(r'(</style>)',fr'\1\n{javascript_multisearch_function}\n', new_html)
    # Add table footer
    table_headers = re.search(r'<thead>.*</thead>', ms_table).group()
    table_foot = table_headers.replace('thead', 'tfoot')
    ms_table = re.sub(r'</tbody>',rf'</tbody>\n{table_foot}\n', ms_table)
    # Duplicate table columns th (required for the javasript code)
    ms_table = re.sub(r'<thead>(.*)</thead>',r'<thead>\1\n\1</thead>\n', ms_table)

    return ms_table

def addMultiSelectBox(html_vms, df_columns):
    """Add multiselect checkbox to show/hide table columns.
    
    Parameters
    ----------
    html_vms : string
        HTML code
    df_columns : Serie 
        Contains a list of dataframe column headers

    Returns
    -------
    html_vms
        Input HTML code with additional code to implement multiselect checkbox

    http://multiple-select.wenzhixin.net.cn/examples#basic.html
    """

    option = '\n'
    for i in range(len(df_columns)):
        option += rf'<option value="{i}">{df_columns[i]}</option>\n'


    javascript_multiselect_function = """

    <!-- Multiple Select JS -->
    <link href="https://unpkg.com/multiple-select@1.5.2/dist/multiple-select.min.css" rel="stylesheet">
    <script src="https://unpkg.com/multiple-select@1.5.2/dist/multiple-select.min.js"></script>

    <div class="form-group row">
        <div class="col-sm-10">
            Columns to display: &nbsp;&nbsp;
        <select multiple="multiple" class="multiple-select" id="ms" name="ms">
            {optionList}
        </select>
        </div>
        <br>
    </div>


    <script>
        function hideAllColumns() {{
            for(var i=0;i<{length};i++) {{
                columns = table.columns(i).visible(0);
            }}
        }}
        function showAllColumns() {{
            for(var i=0;i<{length};i++) {{
                columns = table.columns(i).visible(1);
            }}
        }}

        $(function() {{
            $('.multiple-select').multipleSelect({{
                onClick: function(view){{
                    var selectedItems = $('#ms').multipleSelect("getSelects");
                    hideAllColumns();
                    for(var i=0;i<selectedItems.length;i++){{
                        var s = selectedItems[i];
                        table.columns(s).visible(1);
                    }}
                }},
                onCheckAll: function() {{
                    hideAllColumns();
                    showAllColumns();
                }},
                onUncheckAll: function(){{
                    hideAllColumns();
                }}
            }})
    }})
    </script>
    """.format(optionList = option, length = len(df_columns))

    html_vms = re.sub(r'(</style>)',fr'\1\n{javascript_multiselect_function}\n', html_vms)

    return html_vms

def addPerColumnToolTip(html, df_name, cpld_valid = '', idrac_valid = '', bios_valid = '', esxi_build_valid = '', ism_valid = '1949', nic_i40en_valid_driver = '', nic_i40en_valid_firmware = ''):
    """Add a tooltip description to each table column header."""

    column_title_df_hosts = {'Host_Name': 'ESXi host name', 
                    'MOID': 'vSphere host unique ID', 
                    'Cluster_Name': 'Cluster to which the ESXi host belongs',
                    'Provisioned_vCPUs': 'Summatory of vCPUs configured for all the VMs running in this host\nCALCULATED AS: vcpus_regular_vms + vcpus_latency_vms*2',
                    'Provisioned_RAM': 'Summatory of RAM configured for all the VMs running in this host in GB',
                    'Datastore_Provisioned_GB': 'Summatory of vHDD space configured for all the VMs running in this host',
                    'RealTime_vCPUs': 'Summatory of vCPUs configured for RealTime VMs (RP_GOLD)', 
                    'RealTime_Occupation_Perc': 'Percentage of host threads configured in RealTime VMs vCPUs (RP_GOLD)\nCALCULATED AS: RealTime_vCPUs*100/host_total_pcpus (If SMT is active pCPU means Thread/LCPU. If SMT is disabled pCPU means Core)\nHIGHLIGHTED IN RED IF: RealTime_Occupation_Perc > Max_RealTime_Occupation_Perc',
                    'Total_CPU_Occupation_Perc': 'Percentage of host threads configured in all its VMs vCPUs\nCALCULATED AS: (Provisioned_vCPUs + hypervisor_reserved_pcpus)*100/host_total_pcpus (If SMT is active pCPU means Thread/LCPU. If SMT is disabled pCPU means Core)\nHIGHLIGHTED IN RED IF: Total_CPU_Occupation_Perc > Max_OverProv_Ratio_Perc',
                    'Total_RAM_Occupation_Perc': 'Percentage of host RAM configured in the vRAM of all its VMs\nCALCULATED AS: (Provisioned_RAM + hypervisor_reserved_RAM_GB)*100/host_total_ram\nHIGHLIGHTED IN RED IF: Total_RAM_Occupation_Perc > 100', 
                    'Max_RealTime_Occupation_Perc': 'Maximum percentage of vCPUs allowed to be configured for RealTime VMs (RP_GOLD) in the host', 
                    'Max_OverProv_Ratio_Perc': 'Maximum oversubscription ratio allowed for the host.\nDepends on host cluster type: A=100%, B,C,D=300%',
                    'Socket0_Pinned_vCPUs': 'Summatory of vCPUs configured for all the VMs pinned with NUMA Affinity to this Socket\nCALCULATED AS: vcpus_socket_regular_vms + vcpus_socket_latency_vms*2',
                    'Socket0_CPU_Occupation_Perc': 'Percentage of socket threads configured in vCPUs of VMs pinned with NUMA Affinity to this Socket\nCALCULATED AS: Socket0_Pinned_vCPUs*100/socket_total_pcpus\nHIGHLIGHTED IN RED IF: Socket0_CPU_Occupation_Perc > 100',
                    'Socket1_Pinned_vCPUs': 'Summatory of vCPUs configured for all the VMs pinned with NUMA Affinity to this Socket',
                    'Socket1_CPU_Occupation_Perc': 'Percentage of socket threads configured in vCPUs of VMs pinned with NUMA Affinity to this Socket\nCALCULATED AS: Socket0_Pinned_vCPUs*100/socket_total_pcpus\nHIGHLIGHTED IN RED IF: Socket0_CPU_Occupation_Perc > 100',
                    'Socket0_Pinned_vMEM': 'Summatory of vRAM configured for all the VMs pinned with NUMA Affinity to this Socket',
                    'Socket0_RAM_Occupation_Perc': 'Percentage of socket threads configured in vRAM of VMs pinned with NUMA Affinity to this Socket\nCALCULATED AS: Socket0_Pinned_vMEM*100/socket_total_ram\nHIGHLIGHTED IN RED IF: Socket0_RAM_Occupation_Perc > 100',
                    'Socket1_Pinned_vMEM': 'Summatory of vRAM configured for all the VMs pinned with NUMA Affinity to this Socket',
                    'Socket1_RAM_Occupation_Perc': 'Percentage of socket threads configured in vRAM of VMs pinned with NUMA Affinity to this Socket\nCALCULATED AS: Socket0_Pinned_vMEM*100/socket_total_ram\nHIGHLIGHTED IN RED IF: Socket0_RAM_Occupation_Perc > 100',
                    'SRIOV_VMs': 'Number of VMs running in the host with at least one SRIOV interface',
                    'SRIOV_VFs_Provisioned': 'Total number of SRIOV VFs configured to VMs running in this host',
                    'PCIPT_VMs': 'Number of VMs running in the host with at least one PCI Passthrough interface',
                    'PCIPT_Devices_Provisioned': 'Total number of PCIPT interfaces configured to VMs running in this host',
                    'Datastore_Name': 'ESXi main datastore name\nCALCULATED AS: type=VMFS & "_local" in Datastore_Name',
                    'Datastore_Capacity_GB': 'Total datastore capacity in GB', 
                    'Datastore_Free_GB': 'Actual free datastore space in GB',
                    'Datastore_ProvisionedSwap_GB': 'Summatory of not reserved RAM of all the VMs running in this host in GB',
                    'Datastore_MixedSpace_GB': 'Datastore available space for VM snapshots and restorations (according to established criteria)\nCALCULATED AS: Datastore_Capacity_GB - Datastore_Provisioned_GB - Datastore_ProvisionedSwap_GB',
                    'ESXi_Version': 'ESXi major version number',
                    'ESXi_Build': 'ESXi build number\nHIGHLIGHTED IN RED IF: not in ' + str(esxi_build_valid).replace("'",""), 
                    'BIOS_Version': 'Host BIOS version\nHIGHLIGHTED IN RED IF: not in ' + str(bios_valid).replace("'",""), 
                    'CPLD_Version': 'Host CPLD version (retrieved from iDRAC)\nHIGHLIGHTED IN RED IF: not in ' + str(cpld_valid).replace("'",""), 
                    'iDRAC_Version': 'Host iDRAC version (retrieved from iDRAC)\nHIGHLIGHTED IN RED IF: not in ' + str(idrac_valid).replace("'",""),
                    'VIB_ISM_Version': 'ESXi ISM VIB version (retrieved from esxcli)\nHIGHLIGHTED IN RED IF: not in ' + str(ism_valid).replace("'",""),
                    'ESXi_Rsv_Cores': 'Amount of full Cores reserved by hypervisor processes',
                    'ESXi_Rsv_RAM_GB': 'Amount of RAM reserved by hypervisor processes in GB',
                    'Model': 'Host hardware model',
                    'timestamp': 'Data retrieval time'}

    column_title_df_hosts_network={'Host_Name':'ESXi host name',
                                    'MOID':'vSphere host unique id',
                                    'vmnic_Name': 'vmnic ID',
                                    'vmnic_Model': 'vmnic model',
                                    'vmnic_Driver': 'vmnic name',
                                    'vmnic_Driver_version': 'vmnic driver version\nHIGHLIGHTED IN RED IF: not in ' + str(nic_i40en_valid_driver).replace("'",""),
                                    'vmnic_Firmware_version': 'vmnic firware version\nHIGHLIGHTED IN RED IF: not in ' + str(nic_i40en_valid_firmware).replace("'",""),
                                    'vmnic_MAC': 'The MAC address of this vmnic', 
                                    'vmnic_Device': 'Bus/Device/Function of the PCI device corresponfing to this vmnic',
                                    'vmnic_Type': 'vmnic usage: [dVS, vSwitch, SR-IOV, PCI-PT]',
                                    'vmnic_Link_Status': 'Physical link status\nHIGHLIGHTED IN RED IF: down',
                                    'vmnic_Configured_Speed_Mbps': 'vmnic configured speed: [Auto, 10000, 20000...]',
                                    'vmnic_NUMA': 'The NUMA Node in which this vmnic is physically installed',
                                    'vmnic_virtualSwitch': 'vSwitch in which the vmnic is used',
                                    'vmnic_configured_VFs': 'Number of VFs configured in SRIOV vmnics\nHIGHLIGHTED IN RED IF: value != 0 and vmnic_Type != SRIOV',
                                    'Host_calculated_VF_Vector': 'The SRIOV vector that must be configured in the esxcli according to each vmnic_Type and vmnic_configured_VFs', 
                                    'Host_current_VF_Vector': 'The actual SRIOV vector currently present in the esxcli (retrieved from esxcli)\nHIGHLIGHTED IN RED IF: Host_current_VF_Vector != Host_calculated_VF_Vector',
                                    'Host_calculated_Trusted_Vector': 'The SRIOV Trusted vector that must be configured in the esxcli according to each vmnic_Type and vmnic_configured_VFs',                                   
                                    'Host_current_Trusted_Vector': 'The actual SRIOV Trusted vector currently present in the esxcli (retrieved from esxcli)\nHIGHLIGHTED IN RED IF: (Host_current_Trusted_Vector != Host_calculated_Trusted_Vector) AND (vmnic_Driver_version == 1.10.6) AND (vmnic_Driver == i40en)',
                                    'iDRAC_NIC_Slot': 'NIC Slot of this vmnic (as reported by iDRAC)',
                                    'iDRAC_EthernetPort_Slot': 'Port Slot of this vmnic (as reported by iDRAC)', 
                                    'physical_Switch_name': 'GSW to which this vmnic is connected', 
                                    'physical_Switch_port': 'GSW port to which this vmnic is connected', 
                                    'physical_Switch_port_VLANs': 'List of VLANs configured in the GSW port to which this vmnic is connected', 
                                    'vmnic_max_VFs': 'The maximum number of VFs configurable in the vmnic as specified by the host BIOS settings', 
                                    'Model': 'Host hardware model', 
                                    'Cluster_Name': 'Cluster to which the ESXi host belongs',
                                    'timestamp': 'Data retrieval time'}


    column_title_df_vms = {'VM_Name': 'VM name\nHIGHLIGHTED IN RED IF: naming does not match mandatory pattern',
                            'MOID': 'vSphere VM unique ID',
                            'Host_Name': 'ESXi host name',
                            'VM_vCPU': 'Number of vCPUs configured to this VM',
                            'VM_vMEM_GB': 'Amount of RAM configured to this VM in GB',
                            'VM_Provisioned_Storage_GB': 'Aggregated provisioned capacity for a given VM in GB',
                            'VM_Space_In_Disk_GB': 'Actual space in disk used by this VM in GB',
                            'VM_RealTime': 'Indicates if the VM is RealTime (RP_GOLD) or not (RP_SILVER)',
                            'VM_LatencySensitivity': 'Latency Sensitivity value configured to this VM',
                            'VM_CoresPerSocket': 'CoresPerSocket value configured to this VM\nHIGHLIGHTED IN RED IF: (VM has NUMA Affinity and VM_CoresPerSocket != VM_vCPU) or (VM has not NUMA Affinity and odd(VM_vCPU) and VM_CoresPerSocket != VM_vCPU) or (VM has not NUMA Affinity and even(VM_vCPU) and VM_CoresPerSocket != VM_vCPU/2)',
                            'VM_NUMA': 'If NUMA Affinity is configured this field indicated the NUMA Node to which the VM execution is bound',
                            'CPU_Reservation_MHz': 'Amount or reserved MHz to this VM\nHIGHLIGHTED IN RED IF: not 0 and VM is not LS=high\nHIGHLIGHTED IN RED IF: not full MHz Reservation and VM is LS=high',
                            'RAM_Reservation_GB': 'Amount of reserved RAM to this VM in GB\nHIGHLIGHTED IN RED IF: not 0 and VM is not LS=high or has not SRIOV/PCIPT vNICs\nHIGHLIGHTED IN RED IF: not full RAM Reservation and VM is LS=high or has SRIOV/PCIPT vNICs',
                            'SRIOV_vNICs': 'Number of SRIOV vmnic configured in this VM',
                            'VMXNET3_vNICs': 'Number of vmxnet3 vmnic configured in this VM',
                            'PCIPT_vNICs': 'Number of PCIPT vmnic configured in this VM',
                            'VM_SwapFile_Size_GB': 'Size of the memory swap file generated for this VM (same as VM unreserved memory)',
                            'Cluster_Name': 'Name of the Cluster to which the VM parent Host belongs',
                            'Datastore_Name': 'Name of the Datastore in which VM files are hosted',
                            'Datastore_Capacity_GB': 'Total datastore capacity in GB',
                            'Datastore_Free_GB': 'Actual free datastore space in GB',
                            'VM_Provisioned_vHDDs': 'Number of vHDDs configured to this VM',
                            'VM_Snapshot': 'Whether the VM has any snapshot or not\nHIGHLIGHTED IN RED IF: not True',
                            'Restoration_Allowed': 'Indicates if a restoration from backup request in the same host is allowed for this particular VM\nCALCULATED AS: Datastore_MixedSpace_GB > VM_Space_In_Disk_GB + total_snapshots_inHost_disk_usage',
                            'Snapshot_Allowed': 'Indicates if a snapshot request in the same host is allowed for this particular VM\nCALCULATED AS: Datastore_MixedSpace_GB > VM_Space_In_Disk_GB',
                            'VM_PowerState': 'VM power status: [ON, OFF]',
                            'VM_AntiAffinity': 'List of VMs which cannot be hosted in the same host',
                            'VM_Affinity': 'List of VMs which must be hosted in the same host',
                            'VM_AR_Rule_Compliant': 'Whether the set of configured (anti)affinity rules are observed or not\nHIGHLIGHTED IN RED IF: not True',
                            'VM_SP_Label': 'Name of the serial port device configured to the VM',
                            'VM_SP_proxyURI': 'Serial Port server listening URL',
                            'VM_SP_serviceURI': 'Serial Port server listening application',
                            'VM_SP_direction': 'Serial Port device direction',
                            'Host_CPU_Package_MHz': 'The nominal speed of each host CPU Package',
                            'VirtualHardware_Version': 'VM virtual hardware version',
                            'UUID': '',
                            'Host_MOID': 'vSphere host unique ID',
                            'timestamp': 'Data retrieval time'}

    column_title_df_vms_network ={'VM_Name': 'VM name\nHIGHLIGHTED IN RED IF: naming does not match mandatory pattern',
                                    'vNIC_Name': 'Name of the Network port device configured to the VM',
                                    'vNIC_Type': 'Type of the vNIC: [SR-IOV, e1000, vmxnet3, PCI-PT]',
                                    'vNIC_DPG': 'Distributed Port group configured to this vNIC',
                                    'vNIC_VLANs': 'VLANs configured in the DPG assigned to this vNIC',
                                    'vNIC_MAC': 'MAC address configured at vCenter for this vNIC (if a different MAC is configured inside the GuestOS it will not be reported in this column)',
                                    'pNIC_inUse': 'Actual vmnic managing vNIC traffic (as reported by esxcli)',
                                    'pNIC_inUse_NUMA': 'NUMA Node of the vmnic managing vNIC traffic (as reported by esxcli)\nHIGHLIGHTED IN RED IF: VM has NUMA affinity and its NUMA Node is not the same as the NUMA Node of its vmnic (applies to SRIOV and PCIPT vmnics)',
                                    'vNIC_GuestOS_Mapping_Order': 'Ordered list of vNIC to GuestOS mapping according to vNIC_pciSlotNumber values',
                                    'vNIC_pciSlotNumber': 'The pciSlotNumber assigned to this vNIC (as reported in .vmx file or VM advanced parameters)',
                                    'vNIC_rxBuffer_Ring1_bytes': 'vNIC Rx ring buffer size in bytes',
                                    'vNIC_rxBuffer_Ring1_fullTimes': 'The amount of times this vNIC has encountered a full Rx ring buffer',
                                    'vNIC_SRIOV_VF_ID': 'The VF ID of the VF assigned to a SRIOV vNIC',
                                    'pNIC_PCI_Device': 'For SRIOV vNICs this fields indicates the exact PCI Device backing up the vNIC',
                                    'PortMirror_Session_Source': 'Name of the Port Mirror Session in which this vNIC is defined as traffic source (inbound|outbound|both)',
                                    'DPG_Active_Uplinks': 'Active uplinks configured in the DPG assigned to this vNIC',
                                    'DPG_Standby_Uplinks': 'Standby uplinks configured in the DPG assigned to this vNIC',
                                    'DPG_Promiscuous_Mode': 'Promiscuous_Mode value configured to the DPG assigned to this vNIC',
                                    'DPG_MAC_Address_Changes': 'MAC_Address_Changes value configured to the DPG assigned to this vNIC',
                                    'DPG_Forged_Transmits': 'Forged_Transmits value configured to the DPG assigned to this vNIC',
                                    'DPG_Load_Balancing': 'Load Balancing Policy value configured to the DPG assigned to this vNIC',
                                    'vNIC_dVS': 'dVS to which the DPG assigned to this vNIC belongs',
                                    'dVS_LLDP': 'LLDP value configured in the dVS to which this vNIC belongs',
                                    'MOID': 'vSphere VM unique ID',
                                    'Host_Name': 'Name of the ESXi hosting this VM',
                                    'VM_NUMA': 'If NUMA Affinity is configured this field indicated the NUMA Node to which the VM execution is bound'}

    if df_name == 'df_vms':
        column_title = column_title_df_vms
    elif df_name == 'df_vms_network':
        column_title = column_title_df_vms_network
    elif df_name == 'df_hosts':
        column_title = column_title_df_hosts
    elif df_name == 'df_hosts_network':
        column_title = column_title_df_hosts_network

    for field in column_title:
        html = re.sub(rf'(>{field}<)',rf" title='{column_title[field]}'\1", html)

    return html

def addColumnDraggable(new_html, tableName):
    """Make table columns draggable.
    
    Parameters
    ----------
    new_html : string
        HTML code
    tableName : string
        HTML table name
    Returns
    -------
    draggable_table
        Input HTML code with JS script reference to make table columns draggable
    """

    js_url = "http://www.danvk.org/dragtable/dragtable.js"
    js_script = f'<script src={js_url}></script>'

    draggable_table = re.sub(fr'(table id="T_{tableName}")',fr'\1 class="draggable"', new_html)
    draggable_table = re.sub(r'(</style>)',fr'\1\n{js_script}\n', draggable_table)

    return draggable_table

def addSeachField(new_html, tableName):
    """Add a HTML seach input field.
    
    Parameters
    ----------
    new_html : string
        HTML code
    tableName : string
        HTML table name
    Returns
    -------
    searchable_table
        Input HTML code with JS, CSS and HTML code to add a seach input field
    """
    # As we are using "format" to embed a variable in the JS code, the curly braces in that code must be doubled so that they are scaped.
    # Doubling up those curly braces escapes them; the final output will contain single { and } characters again
    js_search_function = """
    		<script>
                function searchFunction() {{
                // Declare variables
                var input, filter, table, tr, td, i, txtValue;
                input = document.getElementById("myInput");
                filter = input.value.toUpperCase();
                table = document.getElementById("T_{table}");
                tr = table.getElementsByTagName("tr");

                // Loop through all table rows, and hide those who don't match the search query
                for (i = 0; i < tr.length; i++) {{
                    td = tr[i].getElementsByTagName("td")[0];
                    if (td) {{
                    txtValue = td.textContent || td.innerText;
                    if (txtValue.toUpperCase().search(filter) > -1) {{
                        tr[i].style.display = "";
                    }} else {{
                        tr[i].style.display = "none";
                    }}
                    }}
                }}
                }}
            </script>
    """.format(table = tableName)

    css_input_field = """
    	#myInput {
            background-image: url("https://img.icons8.com/metro/26/000000/search.png"); /* Add a search icon to input */
            background-position: 10px 8px; /* Position the search icon */
            background-repeat: no-repeat; /* Do not repeat the icon image */
            width: 50%; /* Full-width */
            font-size: 14px; /* Increase font-size */
            padding: 12px 20px 12px 40px; /* Add some padding */
            border: 1px solid #ddd; /* Add a grey border */
            margin-bottom: 12px; /* Add some space below the input */
        }
    """

    if "Host" in tableName:
        var = "Host"
    elif "VM" in tableName:
        var = "VM"

    html_input_field = f'<input type="text" id="myInput" onkeyup="searchFunction()" placeholder="Search for {var} names (REGEX)...">'
    
    searchable_table = new_html + '\n' + js_search_function
    searchable_table = re.sub(r'(</style>)',fr'{css_input_field}\n\1\n{html_input_field}\n', searchable_table)

    return searchable_table

def output_json_for_splunk(df):
    
    # Reorder dataframe so that timestamp is the first column
    col_timestamp_name = 'timestamp'
    col_timestamp = df.pop(col_timestamp_name) # Remove timestamp column from dataframe
    df.insert(0, col_timestamp_name, col_timestamp) # Insert timestamp column in first position of dataframe
    
    #df_hosts_json = df_hosts[['timestamp', 'Host_Name', 'MOID', 'Cluster_Name', 'Provisioned_vCPUs', 'Provisioned_RAM', 'Datastore_Provisioned_GB', 'RealTime_vCPUs', 
    #                    'RealTime_Occupation_Perc', 'Total_CPU_Occupation_Perc', 'Total_RAM_Occupation_Perc', 'Max_RealTime_Occupation_Perc', 
    #                    'Max_OverProv_Ratio_Perc', 'Socket0_Pinned_vCPUs', 'Socket0_CPU_Occupation_Perc', 'Socket1_Pinned_vCPUs', 'Socket1_CPU_Occupation_Perc',
    #                    'Socket0_Pinned_vMEM', 'Socket0_RAM_Occupation_Perc', 'Socket1_Pinned_vMEM', 'Socket1_RAM_Occupation_Perc',
    #                    'SRIOV_VMs', 'SRIOV_VFs_Provisioned', 'PCIPT_VMs', 'PCIPT_Devices_Provisioned', 'Datastore_Name', 'Datastore_Capacity_GB', 
    #                    'Datastore_Free_GB', 'Datastore_ProvisionedSwap_GB', 'Datastore_MixedSpace_GB', 'ESXi_Version',
    #                    'ESXi_Build', 'BIOS_Version', 'CPLD_Version', 'iDRAC_Version', 'VIB_ISM_Version', 'ESXi_Rsv_Cores', 'ESXi_Rsv_RAM_GB', 'Model']]

    #Column names to lowercase
    df.columns = map(str.lower, df.columns)

    return df

def writeOuputDataframes(vcenter_ip, queryObject, queryName, df_vms=pd.DataFrame(), df_vms_network=pd.DataFrame(), df_hosts=pd.DataFrame(), df_hosts_network=pd.DataFrame(), df_clusters=pd.DataFrame(), df_datacenters=pd.DataFrame()):
    """Write output dataframes to HTML and CSV files.
    
    Parameters
    ----------
    df_vms : Dataframe (optional)
        Dataframe with data from all analyzed VMs. One VM per row
    df_vms_network: Dataframe (optional)
        Dataframe with networking data from each vNIC in a VM. One vNIC per row
    df_host : Dataframe (optional)
        Dataframe with data from all analyzed Hosts. One Host per row
    df_hosts_network: Dataframe (optional)
        Dataframe with pNIC data from all Hosts in this Cluster. One pNIC per row
    df_clusters : Dataframe (optional)
        Dataframe with data about all analyzed Clusters. One Cluster per row
    df_datacenters : Dataframe (optional)
        Dataframe with data about all analyzed Datacenters. One Datacenter per row
    queryObject : string
        vCenter object under analysis as per received arguments
    queryName : string
        vCenter object number under analysis as per received arguments
    """

    #html_tableID = 'myTable'

    current_time = datetime.now(timezone.utc)
    time_suffix = str(current_time.year) + str(current_time.month).zfill(2) + str(current_time.day).zfill(2) + str(current_time.hour).zfill(2) + str(current_time.minute).zfill(2)
    
    if vcenter_ip == '172.24.216.166':
        vcenter_prefix = 'vcenter_pro'
    elif vcenter_ip == '192.168.127.77':
        vcenter_prefix = 'vcenter_pre'
    else:
        vcenter_prefix = 'vcenter_unknown'

    df_vms.fillna('',inplace=True)
    if not df_vms.empty:
        dataframe_type = 'vms_computing'
        html_tableID = 'myVMTable'
        df_vms = df_vms.infer_objects() # Automatically convert each DF column to the appropiate type
        html_vms = (df_vms.style.hide_index()
                                # set_table_styles contains CSS attributes applied to each table element (header, link, etc.) and situation (hover)
                                ## Modify CSS attributes as mouse hovers over table entries
                                ## Modify CSS attributes for Text Header (dataframe column names)
                                # Green background for column names: ('background-color', '#4CAF50')
                                .set_table_styles([{'selector': 'th', 'props': [('background-color', 'white'),('color', 'black'),('padding', '5px'),('font-size', '11pt'), ('cursor', 'pointer')]},
                                                    #{'selector': 'tr:nth-child(even)', 'props': [('background-color', '#f2f2f2')]},
                                                    #{'selector': 'tr:nth-child(odd)', 'props': [('background-color', 'lightgray')]},
                                                    {'selector': 'tr:hover', 'props': [('background-color', 'gold')]},
                                                    {'selector': 'tr', 'props': [('font-size', '11pt'), ('background-color', 'White')]}
                                                    ])                                 
                                .set_properties(**{'text-align': 'right', 'border-color': 'grey', 'border-style': 'solid', 'border-width': '1px', 'white-space': 'nowrap'})  # Set some table properties. "nowrap" avoids cell content to be truncated in several lines when string contains space or '-'
                                #.highlight_max(color='orange')
                                .apply(lambda x: ["background-color: YellowGreen" for index, value in enumerate(x)], axis = 0, subset=['VM_Name'])
                                #.apply(lambda x: ["color: white" for index, value in enumerate(x)], axis = 0, subset=['VM_Name'])
                                .bar(subset=['VM_vCPU'], color='#08D8C3')
                                .bar(subset=['VM_vMEM_GB'], color='lightgreen')
                                .bar(subset=['VM_Provisioned_Storage_GB'], color='#0855D8')
                                .bar(subset=['VM_Space_In_Disk_GB'], color='deepskyblue')
                                .bar(subset=['SRIOV_vNICs'], color='#D1D86C')
                                .bar(subset=['VMXNET3_vNICs'], color='moccasin')
                                .bar(subset=['PCIPT_vNICs'], color='aquamarine')
                                .apply(lambda x: ["background-color: red" if (value.lower() != 'true') else "" for index, value in enumerate(x)], axis = 0, subset=['VM_AR_Rule_Compliant']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value.lower() != 'false') else "" for index, value in enumerate(x)], axis = 0, subset=['VM_Snapshot']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value != 0 and df_vms.at[index, 'VM_LatencySensitivity'] == 'normal' and df_vms.at[index, 'SRIOV_vNICs'] == 0 and df_vms.at[index, 'PCIPT_vNICs'] == 0) else "" for index, value in enumerate(x)], axis = 0, subset=['RAM_Reservation_GB']) # Red background if any RAM reservation and VM is not LS or has not SRIOV/PCIPT vNICs
                                .apply(lambda x: ["background-color: red" if (value != df_vms.at[index, 'VM_vMEM_GB'] and df_vms.at[index, 'VM_LatencySensitivity'] == 'high') or (value != df_vms.at[index, 'VM_vMEM_GB'] and df_vms.at[index, 'SRIOV_vNICs'] != 0) or (value != df_vms.at[index, 'VM_vMEM_GB'] and df_vms.at[index, 'PCIPT_vNICs'] != 0) else "" for index, value in enumerate(x)], axis = 0, subset=['RAM_Reservation_GB']) # Red background if not full RAM Reservation and VM is LS or has SRIOV/PCIPT vNICs
                                .apply(lambda x: ["background-color: red" if (value != 0 and df_vms.at[index, 'VM_LatencySensitivity'] == 'normal') else "" for index, value in enumerate(x)], axis = 0, subset=['CPU_Reservation_MHz']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value != (df_vms.at[index, 'VM_vCPU']*df_vms.at[index, 'Host_CPU_Package_MHz']) and df_vms.at[index, 'VM_LatencySensitivity'] == 'high') else "" for index, value in enumerate(x)], axis = 0, subset=['CPU_Reservation_MHz']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value != df_vms.at[index, 'VM_vCPU'] and df_vms.at[index, 'VM_NUMA'] != '') or (df_vms.at[index, 'VM_vCPU']%2>0 and value != df_vms.at[index, 'VM_vCPU'] and df_vms.at[index, 'VM_NUMA'] == '' ) or (df_vms.at[index, 'VM_vCPU']%2==0 and value != df_vms.at[index, 'VM_vCPU']/2 and df_vms.at[index, 'VM_NUMA'] == '') else "" for index, value in enumerate(x)], axis = 0, subset=['VM_CoresPerSocket']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (not re.match('VM_[A-Z]{4}[1-9]{1}_[A-Z]{5}_[A-Z0-9.-]{1,16}_[0-9]{2}',value)) else "" for index, value in enumerate(x)], axis = 0, subset=['VM_Name']) # Red background for VMs with an incorrect VM naming
                                .apply(lambda x: ["background-color: red" if (value and len(df_vms[df_vms['UUID'] == value]) > 1) else "" for index, value in enumerate(x)], axis = 0, subset=['UUID'])
                                .set_uuid(html_tableID)
                                .render())  # Render the built up styles to HTML

        #html_vms = tableColumnHideShow(html_vms)
        #html_vms = sliderCheckboxesHideShow(html_vms, df_vms)
        #html_vms = addSortFunctionJs(html_vms, html_tableID)
        html_vms = addStickyHeaderCSS(html_vms)
        html_vms = addMultiSelectBox(html_vms, df_vms.columns)
        html_vms = addMultiSearch(html_vms, html_tableID)
        html_vms = addPerColumnToolTip(html_vms, 'df_vms')

        output_file_name = vcenter_prefix + '.' + dataframe_type + '.' + queryName + '.' + time_suffix
        #dataframe_vm_file = queryObject + '_' + queryName + '_VMs_' + time_suffix
        with open(output_file_name + '.html', 'w') as file:   # Write resulting HTML code to file
            file.write(html_vms)

        df_vms.to_csv(output_file_name + '.csv', index=False)   # Write output Dataframe to CSV file
        # Function to apply specific changes to output JSON so that it an be imported seamlessly into Splunk
        df_vms_json = output_json_for_splunk(df_vms)
        df_vms_json.to_json(output_file_name + '.json', orient='records', lines=True)   # Write output Dataframe to JSON file

        print()
        print(f'{output_file_name} CSV/JSON/HTML files saved in current directory.')


    df_vms_network.fillna('',inplace=True)
    if not df_vms_network.empty:
        dataframe_type = 'vms_networking'
        html_tableID = 'myVMNetworkingTable'
        df_vms_network = df_vms_network.infer_objects() # Automatically convert each DF column to the appropiate type
        df_vms_network['vNIC_pciSlotNumber'] = df_vms_network['vNIC_pciSlotNumber'].astype(int, errors = 'ignore')
        df_vms_network['vNIC_GuestOS_Mapping_Order'] = df_vms_network['vNIC_GuestOS_Mapping_Order'].astype(int, errors = 'ignore')
        html_vms_network = (df_vms_network.style.hide_index()
                                # set_table_styles contains CSS attributes applied to each table element (header, link, etc.) and situation (hover)
                                ## Modify CSS attributes as mouse hovers over table entries
                                ## Modify CSS attributes for Text Header (dataframe column names)
                                # Green background for column names: ('background-color', '#4CAF50')
                                .set_table_styles([{'selector': 'th', 'props': [('background-color', 'white'),('color', 'black'),('padding', '5px'),('font-size', '11pt'), ('cursor', 'pointer')]},
                                                    #{'selector': 'tr:nth-child(even)', 'props': [('background-color', '#f2f2f2')]},
                                                    #{'selector': 'tr:nth-child(odd)', 'props': [('background-color', 'lightgray')]},
                                                    {'selector': 'tr:hover', 'props': [('background-color', 'gold')]},
                                                    {'selector': 'tr', 'props': [('font-size', '11pt'), ('background-color', 'White')]}
                                                    ])                                 
                                .set_properties(**{'text-align': 'right', 'border-color': 'grey', 'border-style': 'solid', 'border-width': '1px', 'white-space': 'nowrap'})  # Set some table properties. "nowrap" avoids cell content to be truncated in several lines when string contains space or '-'
                                #.highlight_max(color='orange')
                                .apply(lambda x: ["background-color: YellowGreen" for index, value in enumerate(x)], axis = 0, subset=['VM_Name'])
                                .apply(lambda x: ["background-color: lightblue" if (value == "vmxnet3") else "" for index, value in enumerate(x)], axis = 0, subset=['vNIC_Type'])
                                .apply(lambda x: ["background-color: lightgreen" if (value == "SR-IOV") else "" for index, value in enumerate(x)], axis = 0, subset=['vNIC_Type'])
                                .apply(lambda x: ["background-color: yellow" if (value == "PCI-PT") else "" for index, value in enumerate(x)], axis = 0, subset=['vNIC_Type'])
                                .apply(lambda x: ["background-color: lightorange" if (value == "e1000") else "" for index, value in enumerate(x)], axis = 0, subset=['vNIC_Type'])
                                .apply(lambda x: ["background-color: red" if (value and df_vms_network.at[index, 'VM_NUMA'] and value!=df_vms_network.at[index, 'VM_NUMA'] and (df_vms_network.at[index, 'vNIC_Type'] == 'SR-IOV' or df_vms_network.at[index, 'vNIC_Type'] == 'PCI-PT')) else "" for index, value in enumerate(x)], axis = 0, subset=['pNIC_inUse_NUMA']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > '100') else "" for index, value in enumerate(x)], axis = 0, subset=['vNIC_rxBuffer_Ring1_fullTimes']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value and value != "Route based on originating virtual port") else "" for index, value in enumerate(x)], axis = 0, subset=['DPG_Load_Balancing'])
                                #.apply(lambda x: ["background-color: red" if (value == "false") else "" for index, value in enumerate(x)], axis = 0, subset=['dVS_LLDP'])
                                #.apply(lambda x: ["background-color: red" if (value and (df_vms_network.at[index, 'vNIC_Type'] == 'e1000' or df_vms_network.at[index, 'vNIC_Type'] == 'vmxnet3')) else "" for index, value in enumerate(x)], axis = 0, subset=['VM_NUMA']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .set_uuid(html_tableID)
                                .render())  # Render the built up styles to HTML

        #html_vms_network = tableColumnHideShow(html_vms_network)
        #html_vms_network = sliderCheckboxesHideShow(html_vms_network, df_vms_network)
        #html_vms_network = addSortFunctionJs(html_vms_network, html_tableID)
        html_vms_network = addStickyHeaderCSS(html_vms_network)
        html_vms_network = addMultiSelectBox(html_vms_network, df_vms_network.columns)
        html_vms_network = addMultiSearch(html_vms_network, html_tableID)
        html_vms_network = addPerColumnToolTip(html_vms_network, 'df_vms_network')

        output_file_name = vcenter_prefix + '.' + dataframe_type + '.' + queryName + '.' + time_suffix
        #dataframe_vm_network_file = queryObject + '_' + queryName + '_VMs_Network_' + time_suffix
        with open(output_file_name + '.html', 'w') as file:   # Write resulting HTML code to file
            file.write(html_vms_network)

        df_vms_network.to_csv(output_file_name + '.csv', index=False)   # Write output Dataframe to CSV file

        # Function to apply specific changes to output JSON so that it an be imported seamlessly into Splunk
        df_vms_network_json = output_json_for_splunk(df_vms_network)
        df_vms_network_json.to_json(output_file_name + '.json', orient='records', lines=True)   # Write output Dataframe to JSON file

        print()
        print(f'{output_file_name} CSV/JSON/HTML files saved in current directory.')


    df_hosts.fillna('',inplace=True)
    if not df_hosts.empty:
        dataframe_type = 'hosts_computing'
        cpld_valid = {'PowerEdge R730' : ['1.1.3'], 'PowerEdge R740' : ['1.1.3'], 'PowerEdge R940' : ['1.0.5']}
        idrac_valid = {'PowerEdge R730' : ['2.70.70.70'], 'PowerEdge R740': ['4.20.20.20', '4.22.00.00'], 'PowerEdge R940' : ['4.10.10.10']}
        bios_valid = {'PowerEdge R730' : ['2.11.0'], 'PowerEdge R740' : ['2.7.7', '2.8.1'], 'PowerEdge R940' : ['2.5.4', '2.6.4']}
        esxi_build_valid = '15256549'
        ism_valid = '1949'

        html_tableID = 'myHostTable'
        df_hosts = df_hosts.infer_objects() # Automatically convert each DF column to the appropiate type
        html_hosts = (df_hosts.style.hide_index()
                                # set_table_styles contains CSS attributes applied to each table element (header, link, etc.) and situation (hover)
                                ## Modify CSS attributes as mouse hovers over table entries
                                ## Modify CSS attributes for Text Header (dataframe column names)
                                .set_table_styles([{'selector': 'th', 'props': [('background-color', 'white'),('color', 'black'),('padding', '5px'),('font-size', '11pt'), ('cursor', 'pointer')]},
                                                    {'selector': 'tr:hover', 'props': [('background-color', 'gold')]},
                                                    {'selector': 'tr', 'props': [('font-size', '11pt'), ('background-color', 'White')]}
                                                    ])   
                                .set_properties(**{'text-align': 'right', 'border-color': 'grey', 'border-style': 'solid', 'border-width': '1px', 'font-size': '11pt', 'white-space': 'nowrap'})  # Set some table properties
                                .apply(lambda x: ["background-color: YellowGreen" for index, value in enumerate(x)], axis = 0, subset=['Host_Name'])
                                .bar(subset=['Provisioned_vCPUs'], color='#08D8C3')
                                .bar(subset=['Provisioned_RAM'], color='lightgreen')
                                .bar(subset=['Datastore_Provisioned_GB'], color='#0855D8')
                                .bar(subset=['RealTime_vCPUs'], color='deepskyblue')
                                .bar(subset=['SRIOV_VMs'], color='#D1D86C')
                                .bar(subset=['Datastore_MixedSpace_GB'], color='lime')
                                .background_gradient(subset=['RealTime_Occupation_Perc'], cmap='Greys')    # Matplotlib colormaps "https://matplotlib.org/examples/color/colormaps_reference.html"
                                .background_gradient(subset=['Total_CPU_Occupation_Perc'], cmap='Blues')
                                .background_gradient(subset=['Total_RAM_Occupation_Perc'], cmap='Greens')
                                .background_gradient(subset=['Socket0_CPU_Occupation_Perc'], cmap='Blues')
                                .background_gradient(subset=['Socket1_CPU_Occupation_Perc'], cmap='Blues')
                                .background_gradient(subset=['Socket0_RAM_Occupation_Perc'], cmap='Greens')
                                .background_gradient(subset=['Socket1_RAM_Occupation_Perc'], cmap='Greens')
                                .apply(lambda x: ["background-color: red" if (value > df_hosts.at[index, 'Max_RealTime_Occupation_Perc']) else "" for index, value in enumerate(x)], axis = 0, subset=['RealTime_Occupation_Perc']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > df_hosts.at[index, 'Max_OverProv_Ratio_Perc']) else "" for index, value in enumerate(x)], axis = 0, subset=['Total_CPU_Occupation_Perc']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > 100) else "" for index, value in enumerate(x)], axis = 0, subset=['Socket0_CPU_Occupation_Perc']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > 100) else "" for index, value in enumerate(x)], axis = 0, subset=['Socket1_CPU_Occupation_Perc']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > 100) else "" for index, value in enumerate(x)], axis = 0, subset=['Total_RAM_Occupation_Perc']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > 100) else "" for index, value in enumerate(x)], axis = 0, subset=['Socket0_RAM_Occupation_Perc']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if (value > 100) else "" for index, value in enumerate(x)], axis = 0, subset=['Socket1_RAM_Occupation_Perc']) # 'apply' method iterates over columns/rows and returns a Serie in variable 'x'
                                .apply(lambda x: ["background-color: red" if ((df_hosts.at[index, 'Model'] in cpld_valid.keys()) and (value not in cpld_valid[df_hosts.at[index, 'Model']])) else "" for index, value in enumerate(x)], axis = 0, subset=['CPLD_Version'])
                                .apply(lambda x: ["background-color: red" if ((df_hosts.at[index, 'Model'] in idrac_valid.keys()) and (value not in idrac_valid[df_hosts.at[index, 'Model']])) else "" for index, value in enumerate(x)], axis = 0, subset=['iDRAC_Version'])
                                .apply(lambda x: ["background-color: red" if ((df_hosts.at[index, 'Model'] in bios_valid.keys()) and (value not in bios_valid[df_hosts.at[index, 'Model']])) else "" for index, value in enumerate(x)], axis = 0, subset=['BIOS_Version'])
                                .apply(lambda x: ["background-color: red" if (value != esxi_build_valid) else "" for index, value in enumerate(x)], axis = 0, subset=['ESXi_Build'])
                                .apply(lambda x: ["background-color: red" if ((value and value.split('-')[1] != ism_valid) or (not value)) else "" for index, value in enumerate(x)], axis = 0, subset=['VIB_ISM_Version'])
                                .set_uuid(html_tableID)
                                .render())  # Render the built up styles to HTML

        #html_hosts = tableColumnHideShow(html_hosts)
        #html_hosts = sliderCheckboxesHideShow(html_hosts, df_hosts)
        #html_hosts = addSortFunctionJs(html_hosts, html_tableID)
        html_hosts = addStickyHeaderCSS(html_hosts)
        html_hosts = addMultiSelectBox(html_hosts, df_hosts.columns)
        html_hosts = addMultiSearch(html_hosts, html_tableID)
        html_hosts = addPerColumnToolTip(html_hosts, 'df_hosts', cpld_valid = cpld_valid, idrac_valid = idrac_valid, bios_valid = bios_valid, esxi_build_valid = esxi_build_valid, ism_valid = ism_valid)

        output_file_name = vcenter_prefix + '.' + dataframe_type + '.' + queryName + '.' + time_suffix
        #dataframe_host_file = queryObject + '_' + queryName + '_Hosts_' + time_suffix
        with open(output_file_name + '.html', 'w') as file: # Write resulting HTML code to file
            file.write(html_hosts)

        df_hosts.to_csv(output_file_name + '.csv', index=False)   # Write output Dataframe to CSV file

        # Function to apply specific changes to output JSON so that it an be imported seamlessly into Splunk
        df_hosts_json = output_json_for_splunk(df_hosts)
        
        df_hosts_json.to_json(output_file_name + '.json', orient='records', lines=True)   # Write output Dataframe to JSON file


        print()
        print(f'{output_file_name} CSV/JSON/HTML files saved in current directory.')

        #print(df_hosts[['Host_Name', 'Model', 'iDRAC_Version']].to_markdown())
        #print(df_hosts[['Host_Name', 'Host_RealTime_vCPUs_Occupation_Perc', 'Host_Max_RealTime_vCPUs_Perc', 'Host_vCPU_Occupation_Perc (Prov + Hyp)', 'Host_Max_vCPU_Occupation_Perc']].to_markdown())

    df_hosts_network.fillna('',inplace=True)
    if not df_hosts_network.empty:
        dataframe_type = 'hosts_networking'
        nic_i40en_valid_driver = {'1.7.17', '1.10.6'}
        nic_i40en_valid_firmware = '180809'

        html_tableID = 'myHostNetworkingTable'
        html_hosts_network = (df_hosts_network.style
                                .hide_index()
                                # set_table_styles contains CSS attributes applied to each table element (header, link, etc.) and situation (hover)
                                ## Modify CSS attributes as mouse hovers over table entries
                                ## Modify CSS attributes for Text Header (dataframe column names)
                                .set_table_styles([{'selector': 'th', 'props': [('background-color', 'white'),('color', 'black'),('padding', '5px'),('font-size', '11pt'), ('cursor', 'pointer')]},
                                                    {'selector': 'tr:hover', 'props': [('background-color', 'gold')]},
                                                    {'selector': 'tr', 'props': [('font-size', '11pt'), ('background-color', 'White')]}
                                                    ])   
                                .set_properties(**{'text-align': 'right', 'border-color': 'grey', 'border-style': 'solid', 'border-width': '1px', 'font-size': '11pt', 'white-space': 'nowrap'})  # Set some table properties
                                .apply(lambda x: ["background-color: YellowGreen" for index, value in enumerate(x)], axis = 0, subset=['Host_Name'])
                                .apply(lambda x: ["background-color: lightblue" if (value == "dVS") else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_Type'])
                                .apply(lambda x: ["background-color: lightgreen" if (value == "SR-IOV") else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_Type'])
                                .apply(lambda x: ["background-color: yellow" if (value == "PCI-PT") else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_Type']) 
                                .apply(lambda x: ["background-color: red" if (value == "down" and (df_hosts_network.at[index, 'vmnic_Type'] != '')) else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_Link_Status'])
                                .apply(lambda x: ["background-color: red" if (value == "Auto" and 'FlexFabric' not in df_hosts_network.at[index, 'vmnic_Model'] and df_hosts_network.at[index, 'vmnic_Type']) else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_Configured_Speed_Mbps'])
                                .apply(lambda x: ["background-color: red" if (value == "Auto" and 'FlexFabric' not in df_hosts_network.at[index, 'vmnic_Model'] and df_hosts_network.at[index, 'vmnic_Type']) else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_Configured_Speed_Mbps'])
                                .apply(lambda x: ["background-color: red" if (value not in nic_i40en_valid_driver and df_hosts_network.at[index, 'vmnic_Driver'] == 'i40en') else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_Driver_version'])
                                .apply(lambda x: ["background-color: red" if (value and (value.split('.')[0].zfill(2) +  value.split('.')[1].zfill(2) + value.split('.')[2].zfill(2) < nic_i40en_valid_firmware)) else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_Firmware_version'])
                                .apply(lambda x: ["background-color: red" if (value and (value != 0) and (df_hosts_network.at[index, 'vmnic_Type'] != 'SR-IOV')) else "" for index, value in enumerate(x)], axis = 0, subset=['vmnic_configured_VFs'])
                                .apply(lambda x: ["background-color: red" if value != df_hosts_network.at[index, 'Host_calculated_VF_Vector'] else "" for index, value in enumerate(x)], axis = 0, subset=['Host_current_VF_Vector'])
                                .apply(lambda x: ["background-color: red" if (value != df_hosts_network.at[index, 'Host_calculated_Trusted_Vector'] and df_hosts_network.at[index, 'vmnic_Driver'] == 'i40en' and df_hosts_network.at[index, 'vmnic_Driver_version'] == '1.10.6') else "" for index, value in enumerate(x)], axis = 0, subset=['Host_current_Trusted_Vector'])
                                .set_uuid(html_tableID)
                                .render())  # Render the built up styles to HTML

        #html_hosts_network = tableColumnHideShow(html_hosts_network)
        #html_hosts_network = sliderCheckboxesHideShow(html_hosts_network, df_hosts_network)
        #html_hosts_network = addSortFunctionJs(html_hosts_network, html_tableID)
        html_hosts_network = addStickyHeaderCSS(html_hosts_network)
        html_hosts_network = addMultiSelectBox(html_hosts_network, df_hosts_network.columns)
        html_hosts_network = addMultiSearch(html_hosts_network, html_tableID)
        html_hosts_network = addPerColumnToolTip(html_hosts_network, 'df_hosts_network', nic_i40en_valid_driver = nic_i40en_valid_driver, nic_i40en_valid_firmware = nic_i40en_valid_firmware)

        output_file_name = vcenter_prefix + '.' + dataframe_type + '.' + queryName + '.' + time_suffix
        #dataframe_host_network_file = queryObject + '_' + queryName + '_Hosts_Network_' + time_suffix
        with open(output_file_name + '.html', 'w') as file: # Write resulting HTML code to file
            file.write(html_hosts_network)

        df_hosts_network.to_csv(output_file_name + '.csv', index=False)   # Write output Dataframe to CSV file

        # Function to apply specific changes to output JSON so that it an be imported seamlessly into Splunk
        df_hosts_network_json = output_json_for_splunk(df_hosts_network)

        df_hosts_network_json.to_json(output_file_name + '.json', orient='records', lines=True)   # Write output Dataframe to JSON file

        print()
        print(f'{output_file_name} CSV/JSON/HTML files saved in current directory.')
        #print(df_hosts_network.to_markdown())

    if not df_clusters.empty:
        html_cluster = ""
        # Code to write Cluster output DF goes here

    if not df_datacenters.empty:
        html_datacenter = ""
        # Code to write Datacenter output DF goes here

    print()

def main():
    """Main function."""

    start_time = time.time()
    atexit.register(lambda: print("Execution time {:.2f} seconds".format(float(time.time()-start_time))))

    args = parse_arguments()
    vcenter_password = getpass.getpass(prompt='Enter vCenter password: ')

    esxi_username = ''
    esxi_password = ''
    idrac_username = ''
    idrac_password = ''
    if args.esxiuser:
        esxi_username = args.esxiuser
        esxi_password = getpass.getpass(prompt='Enter ESXi password: ')
    if args.idracuser:
        idrac_username = args.idracuser
        idrac_password = getpass.getpass(prompt='Enter iDRAC password: ')

    si = connect(args.vcenter_ip, args.vcenter_user, vcenter_password)  # Connect to vCenter
    atexit.register(Disconnect, si)     # Cleanup. Disconnect the session upon normal script termination
    content = si.RetrieveContent()

    pd.set_option('display.max_rows', None) # So that all Dataframe rows are printed to terminal
    pd.set_option('display.max_colwidth', None)   # To not limit dataframe column width and display full cell content in a single line (avoids being truncated in multiple lines within the cell)

    df_vms = pd.DataFrame()
    df_vms_network = pd.DataFrame()
    df_hosts = pd.DataFrame()
    df_hosts_network = pd.DataFrame()
    df_clusters = pd.DataFrame()
    df_datacenters = pd.DataFrame()

    if args.t == 'vm':
        vm_obj = findVMObj(args.n, content)
        df_vms, df_vms_network = vm_scavenger(vm_obj)
    elif args.t == 'host':
        host_obj = findHostObj(args.n, content)
        df_vms, df_vms_network, df_hosts, df_hosts_network = host_scavenger(host_obj, args.gsw, esxi_username, esxi_password, idrac_username, idrac_password)
    elif args.t == 'cluster':
        cluster_obj = findClusterObj(args.n, content)
        df_vms, df_vms_network, df_hosts, df_hosts_network, df_clusters = cluster_scavenger(cluster_obj, args.gsw, esxi_username, esxi_password, idrac_username, idrac_password)
    elif args.t == 'datacenter':
        datacenter_obj_list = findDatacenterObj(args.n, content)
        for datacenter_obj in datacenter_obj_list:
            df_temp_vms, df_temp_vms_network, df_temp_hosts, df_temp_hosts_network, df_temp_clusters, df_temp_datacenters = datacenter_scavenger(datacenter_obj, args.gsw, esxi_username, esxi_password, idrac_username, idrac_password)
            df_vms = df_vms.append(df_temp_vms, ignore_index=True)
            df_vms_network = df_vms_network.append(df_temp_vms_network, ignore_index=True)
            df_hosts = df_hosts.append(df_temp_hosts, ignore_index=True)
            df_hosts_network = df_hosts_network.append(df_temp_hosts_network, ignore_index=True)
            df_clusters = df_clusters.append(df_temp_clusters, ignore_index=True)
            df_datacenters = df_datacenters.append(df_temp_datacenters, ignore_index=True)

    writeOuputDataframes(args.vcenter_ip, args.t, args.n, df_vms, df_vms_network, df_hosts, df_hosts_network, df_clusters, df_datacenters) # Print output DFs

if __name__ == '__main__':
    main()