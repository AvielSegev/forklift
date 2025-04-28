package vsphere

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/konveyor/forklift-controller/pkg/controller/provider/web/base"
	buildv1 "github.com/openshift/api/build/v1"
	imageapi "github.com/openshift/api/image/v1"
	buildclientset "github.com/openshift/client-go/build/clientset/versioned"
	imagev1client "github.com/openshift/client-go/image/clientset/versioned/typed/image/v1"
	"io"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	uploadDirPerm        = 0600
	VddkRoot             = ProvidersRoot + "/" + "vddk" // Route
)

var (
	buildConfigName  = "vddk"
	registryImageTag = "vddk:latest"
	vddkTarFileName  = "vddk.tar.gz"
	uploadDir        = "/tmp/uploads"
	buildLock        sync.Mutex
	isBusy           bool
	waitForDownload  = 15 * time.Second
)

// VddkHandler provides endpoints for VDDK image management.
type VddkHandler struct {
	Handler
}

// AddRoutes registers the VDDK-specific HTTP routes on the given Gin engine.
func (h *VddkHandler) AddRoutes(e *gin.Engine) {
	e.POST(VddkRoot+"/build-image", h.BuildImage)
	e.GET(VddkRoot+"/image-url", h.ImageUrl)
	e.GET(VddkRoot+"/download-tar", h.DownloadVddkTar)
}

// BuildImage receives a VDDK tar file, writes it to disk,
// and triggers an OpenShift BuildConfig to build and push the image.
func (h *VddkHandler) BuildImage(ctx *gin.Context) {
	status, err := h.Prepare(ctx)
	if status != http.StatusOK {
		ctx.Status(status)
		base.SetForkliftError(ctx, err)
		return
	}

	buildLock.Lock()
	if isBusy {
		buildLock.Unlock()
		JSONError(ctx, http.StatusServiceUnavailable, "Server is busy processing another build. Please try again later.")
		return
	}
	isBusy = true
	buildLock.Unlock()
	defer resetBusyAfter(waitForDownload)

	file, err := ctx.FormFile("file")
	if err != nil {
		JSONError(ctx, http.StatusBadRequest, "No file provided")
		return
	}

	src, err := file.Open()
	if err != nil {
		JSONError(ctx, http.StatusInternalServerError, fmt.Sprintf("Could not process uploaded file: %v", err))
		return
	}
	defer src.Close()

	if err := os.MkdirAll(uploadDir, uploadDirPerm); err != nil {
		JSONError(ctx, http.StatusInternalServerError, fmt.Sprintf("Could not prepare upload directory: %v", err))
		return
	}

	filePath := filepath.Join(uploadDir, vddkTarFileName)
	dst, err := os.Create(filePath)
	if err != nil {
		JSONError(ctx, http.StatusInternalServerError,
			fmt.Sprintf("error: %v, Could not save file on disk: %s. ", err, filePath))
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		JSONError(ctx, http.StatusInternalServerError, fmt.Sprintf("error copy to the local file: %v. ", err))
		return
	}

	if err := BuildAndPushImage(); err != nil {
		JSONError(ctx, http.StatusInternalServerError, err.Error())
		return
	}

	JSONSuccess(ctx, "VDDK build started; check your registry in OpenShift", nil)
}

// ImageUrl handles HTTP requests to fetch the VDDK image URL.
// it returns a 200 JSON response containing the image reference. On error,
// it writes a JSON error with the appropriate HTTP status.
func (h *VddkHandler) ImageUrl(ctx *gin.Context) {
	status, err := h.Prepare(ctx)
	if status != http.StatusOK {
		ctx.Status(status)
		base.SetForkliftError(ctx, err)
		return
	}

	namespace, err := currentNamespace()
	if err != nil {
		JSONError(ctx, http.StatusInternalServerError, fmt.Sprintf("Could not determine namespace: %v", err))
		return
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		JSONError(ctx, http.StatusInternalServerError, fmt.Sprintf("Could not load cluster config: %v", err))
		return
	}

	imgClient, err := imagev1client.NewForConfig(cfg)
	if err != nil {
		JSONError(ctx, http.StatusInternalServerError, fmt.Sprintf("Could not create image client: %v", err))
		return
	}

	url, exists, err := imageReference(ctx.Request.Context(), imgClient, namespace, registryImageTag)
	if err != nil {
		JSONError(ctx, http.StatusInternalServerError, fmt.Sprintf("Error checking image reference: %v", err))
		return
	}

	if !exists {
		JSONError(ctx, http.StatusNotFound, fmt.Sprintf("Image: %s not found", registryImageTag))
		return
	}

	JSONSuccess(ctx, fmt.Sprintf("Image: %s exists", registryImageTag), gin.H{"imageReference": url})
}

// DownloadVddkTar streams the uploaded VDDK tar back to the client.
func (h *VddkHandler) DownloadVddkTar(ctx *gin.Context) {
	status, err := h.Prepare(ctx)
	if status != http.StatusOK {
		ctx.Status(status)
		base.SetForkliftError(ctx, err)
		return
	}

	filePath := filepath.Join(uploadDir, vddkTarFileName)

	// Check if file exists
	if _, err := os.Stat(filePath); err != nil {
		if os.IsNotExist(err) {
			JSONError(ctx, http.StatusNotFound, "VDDK tar not found")
		} else {
			JSONError(ctx, http.StatusInternalServerError, fmt.Sprintf("Failed to stat file: %v", err))
		}
		return
	}

	ctx.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%q", vddkTarFileName))
	ctx.Header("Content-Type", "application/octet-stream")

	ctx.File(filePath)
}

// BuildAndPushImage triggers the OpenShift BuildConfig to build and push the VDDK image.
func BuildAndPushImage() error {
	namespace, err := currentNamespace()
	if err != nil {
		return fmt.Errorf("failed to get the pod namespace: %w", err)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load kube config: %w", err)
	}

	buildClient, err := buildclientset.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create build client: %w", err)
	}

	buildRequest := &buildv1.BuildRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: buildConfigName,
		},
	}

	_, err = buildClient.BuildV1().
		BuildConfigs(namespace).
		Instantiate(context.TODO(), buildConfigName, buildRequest, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("start build: %w", err)
	}

	return nil
}

// currentNamespace will return the namespace this pod is running in,
// or an error if it can’t be determined.
func currentNamespace() (string, error) {
	byteContent, err := os.ReadFile(defaultNamespaceFile)
	if err != nil {
		return "", fmt.Errorf("could not read byteContent file: %w", err)
	}
	namespace := strings.TrimSpace(string(byteContent))
	if namespace == "" {
		return "", fmt.Errorf("file was empty")
	}
	return namespace, nil
}

// imageReference returns (image url, true, nil) if the given ImageStreamTag exists,
// ("", false, nil) if it does not, or ("", false, error) on any other failure.
func imageReference(ctx context.Context, getter imagev1client.ImageStreamTagsGetter,
	namespace, registryImageTag string) (string, bool, error) {
	ist := &imageapi.ImageStreamTag{}
	ist, err := getter.ImageStreamTags(namespace).Get(ctx, registryImageTag, metav1.GetOptions{})
	if kerrors.IsNotFound(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("error: %w. could not get ImageStreamTag: %s", err, registryImageTag)
	}
	return ist.Image.DockerImageReference, true, nil
}

// JSONError sends a standardized error response.
func JSONError(ctx *gin.Context, code int, msg string) {
	ctx.JSON(code, gin.H{"status": "error", "message": msg})
}

// JSONSuccess sends a standardized success response with optional data.
func JSONSuccess(ctx *gin.Context, msg string, data gin.H) {
	resp := gin.H{"status": "success", "message": msg}
	if data != nil {
		resp["data"] = data
	}
	ctx.JSON(http.StatusOK, resp)
}

// resetBusy resets the global busy flag, allowing new builds to proceed
func resetBusy() {
	buildLock.Lock()
	isBusy = false
	buildLock.Unlock()
}

// resetBusyAfter schedules a reset of the busy flag after the specified timeout
func resetBusyAfter(timeout time.Duration) {
	go func() {
		time.Sleep(timeout)
		resetBusy()
	}()
}
