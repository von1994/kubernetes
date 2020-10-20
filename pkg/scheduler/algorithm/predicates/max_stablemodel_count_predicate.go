package predicates

import (
	"fmt"
	"strconv"

	// "k8s.io/klog"

	"k8s.io/api/core/v1"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

type NumLimit struct {
	Type	string	`json:"type"`
	Num		int		`json:"unit"`
}

var (
	ErrorLimitAnnotationNotExist	=	newPredicateFailureError(MaxStableModelCountPred, "the node without annotation about StableModel limit")
	ErrorCountAnnotationNotExist	=	newPredicateFailureError(MaxStableModelCountPred, "the node without annotation about StableModel count")
	NodeStableModelLimitAnnotation	=	"stableModel.count.limit"
	NodeStableModelCountAnnotation	=	"stableModel.count.current"
	StableModelKind	= "StableModel"
)

func MaxStableModelCountPredicates(pod *v1.Pod, meta PredicateMetadata, nodeInfo *schedulernodeinfo.NodeInfo) (bool, []PredicateFailureReason, error) {
	if pod.OwnerReferences == nil {
		return true, nil, nil
	}

	for _, v := range pod.OwnerReferences {
		if v.Kind == StableModelKind {
			// Pod归属StableModel，需要检测节点配额限制
			limitStr, ok := nodeInfo.Node().Annotations[NodeStableModelLimitAnnotation]
			if !ok {
				return false, []PredicateFailureReason{ErrorLimitAnnotationNotExist}, nil
			}
			total, _ := strconv.ParseInt(limitStr, 10, 64)

			countStr, ok := nodeInfo.Node().Annotations[NodeStableModelCountAnnotation]
			if !ok {
				return false, []PredicateFailureReason{ErrorCountAnnotationNotExist}, nil
			}
			current, _ := strconv.ParseInt(countStr, 10, 64)

			if current + 1 > total {
				return false, []PredicateFailureReason{newPredicateFailureError(MaxStableModelCountPred,
					fmt.Sprintf("resource StableModel out of count limit %d on the node %s", total, nodeInfo.Node().GetName()))}, nil
			}
		}
	}
	return true, nil, nil
}
