package main
import (
	"time"
	"context"
	"github.com/sirupsen/logrus"
	"k8s.io/client-go/tools/clientcmd"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/dynamic"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func init() {
	logrus.Info("Start milvus operator")
	logrus.Infof("Milvus operator version: %s", "v0.0.1")
}

func main() {
	kubeconfig := "/home/byron/.kube/config"
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Error(err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error(err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Error(err)
	}
	//	createMilvusPod(clientset, "test", "milvusdb/milvus")
	log.Info("Start loop")
	for {
		crdList := milvusList(dynamicClient)
		podList := milvusPods(clientset)
		mainLoop(clientset, crdList, podList)
		time.Sleep(10 * time.Second)
	}

}

func milvusGVR() schema.GroupVersionResource{
	return schema.GroupVersionResource{
		Group: "zilliz.com",
		Version: "v1",
		Resource: "milvuses",
	}
}

func milvusList(client dynamic.Interface) []unstructured.Unstructured{
	milvusInstances, err := client.Resource(milvusGVR()).Namespace("default").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Error(err)
	}
	return milvusInstances.Items
}

func milvusPods(client *kubernetes.Clientset) []v1.Pod{
	pods, err := client.CoreV1().Pods("default").List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Error(err)
	}
	return pods.Items
}

func createMilvusPod(client *kubernetes.Clientset, name string, image string) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "milvus-0",
					Image: image,
				},
			},
		},
	}
	_, err := client.CoreV1().Pods("default").Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		log.Error(err)
	}
}

type MilvusIns struct {
	Name string
	Image string
}

func mainLoop(clientset *kubernetes.Clientset, milvusList []unstructured.Unstructured, pods []v1.Pod) {
	var waitForCreate []MilvusIns
	for _, milvus := range milvusList {
		name := milvus.Object["metadata"].(map[string]interface{})["name"].(string)
		image := milvus.Object["spec"].(map[string]interface{})["image"].(string)
		if existInPods(name, pods) != true {
			waitForCreate = append(waitForCreate, MilvusIns{
				Name: name,
				Image: image,
			})
		}
	}
	for _, m := range waitForCreate {
		createMilvusPod(clientset, m.Name, m.Image)
	}

}

func existInPods(name string, pods []v1.Pod) bool {
	for _, pod := range pods {
		if name == pod.Name {
			return true
		}
	}
	return false
}
