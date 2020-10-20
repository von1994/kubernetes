package predicates

import (
	"encoding/json"
	"fmt"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

type UsageLimit struct {
	MemLimit     int `json:"memory"`
	StorageLimit int `json:"storage"`
	CPULimit     int `json:"cpu"`
	PodNum       int `json:"pod"`
}

var (
	ErrLocalVolumePressure = newPredicateFailureError(CheckLocalVolumePred, "the node(s) has local vg Pressure")
	ErrLocalVolumeNotExist  = newPredicateFailureError(CheckLocalVolumePred, "the node(s) not has related resouce")

	PodLocalVolumeRequestAnnocation = "vg.localvolume.request"
	NodeLocalVolumeAnnocation       = "vg.localvolume.caility"
	// the spell of cability is wrong, but on prod env using this. So not change..
	// localvolume --> localVolume
	// NodeLocalVolumeAnnocation       = "vg.localvolume.cability"

	// 存储资源限制, 剩余可供扩容或其他用途
	StorageLimit	=	80
)

func LocalVolumePredicates(pod *v1.Pod, meta PredicateMetadata, nodeInfo *schedulernodeinfo.NodeInfo) (bool, []PredicateFailureReason, error) {
	requestStr, ok := pod.Annotations[PodLocalVolumeRequestAnnocation]
	if !ok {
		return true, nil, nil
	}

	requestResource, err := parseJsonToResourceList(requestStr)
	if err != nil {
		return false, nil, fmt.Errorf(" parse pod(%s) request fail:%s(data:%s)", pod.GetName(), err.Error(), requestStr)
	}

	//获取主机总资源
	nodeCapability, err := getNodeCapability(nodeInfo)
	if err != nil {
		return false, nil, fmt.Errorf(" %s:getNodeCability fail:%s", nodeInfo.Node().GetName(), err.Error())
	}

	//统计已使用
	nodeUsed, err := countsNodeUsed(nodeInfo)
	if err != nil {
		return false, nil, fmt.Errorf(" %s:countsNodeUsed fail:%s", nodeInfo.Node().GetName(), err.Error())
	}

	for name, quantity := range requestResource {
		nodeQuantity, ok := nodeUsed[name]
		if !ok {
			nodeUsed[name] = quantity
			continue
		}

		nodeQuantity.Add(quantity)
		nodeUsed[name] = nodeQuantity
	}

	//判断资源是否足够
	for name, _ := range requestResource {
		quantityUsed, ok := nodeUsed[name]
		if !ok {
			return false, []PredicateFailureReason{ErrLocalVolumeNotExist}, fmt.Errorf("node:%s not have %s type resouce", nodeInfo.Node().GetName(), name)
		}

		capabilityQuantity, ok := nodeCapability[name]
		if !ok {
			return false, []PredicateFailureReason{ErrLocalVolumeNotExist}, fmt.Errorf("node:%s not have %s type resouce", nodeInfo.Node().GetName(), name)
		}

		if quantityUsed.Value() > capabilityQuantity.Value()*int64(StorageLimit)/100 {
			return false, []PredicateFailureReason{ErrLocalVolumePressure},
			fmt.Errorf("node:%s capability is %d, limit percent %d, current request is %d ", nodeInfo.Node(), StorageLimit, capabilityQuantity.Value(), quantityUsed.Value())
		}
	}

	return true, nil, nil
}

func parseJsonToResourceList(data string) (v1.ResourceList, error) {
	list := v1.ResourceList{}
	resourceReq := map[string]string{}
	err := json.Unmarshal([]byte(data), &resourceReq)
	if err != nil {
		return list, err
	}

	for vg, valueStr := range resourceReq {
		value, err := resource.ParseQuantity(valueStr)
		if err != nil {
			return list, err
		}
		list[v1.ResourceName(vg)] = value
	}

	return list, nil
}

func getNodeCapability(nodeInfo *schedulernodeinfo.NodeInfo) (v1.ResourceList, error) {
	capability := v1.ResourceList{}
	capabilityStr, ok := nodeInfo.Node().Annotations[NodeLocalVolumeAnnocation]
	if !ok {
		return capability, nil
	}
	return parseJsonToResourceList(capabilityStr)
}

func countsNodeUsed(nodeInfo *schedulernodeinfo.NodeInfo) (v1.ResourceList, error) {
	counts := v1.ResourceList{}
	for _, pod := range nodeInfo.Pods() {
		req, ok := pod.Annotations[PodLocalVolumeRequestAnnocation]
		if !ok {
			continue
		}

		reqResource, err := parseJsonToResourceList(req)
		if err != nil {
			return counts, err
		}

		for name, quantity := range reqResource {
			countQuantity, ok := counts[name]
			if !ok {
				counts[name] = quantity
				continue
			}
			countQuantity.Add(quantity)
			counts[name] = countQuantity
		}
	}
	return counts, nil
}

