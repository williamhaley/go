package processing

import (
	"archive/zip"
	"io"
	"os"

	log "github.com/sirupsen/logrus"
)

func pipeline2_1Extract(ctx *ProcessingContext, destinationDirectory string, ins ...<-chan *ProcessingPayload) []<-chan *ProcessingPayload {
	return fanInOrOut(ctx, func(ctx *ProcessingContext, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
		return extractFile(ctx, destinationDirectory, in)
	}, ins...)
}

func extractFile(ctx *ProcessingContext, destinationDirectory string, in <-chan *ProcessingPayload) <-chan *ProcessingPayload {
	out := make(chan *ProcessingPayload)

	log.Debugf("using unzip destination directory %s\n", destinationDirectory)
	if err := os.MkdirAll(destinationDirectory, 0755); err != nil && !os.IsExist(err) {
		log.WithError(err).Fatal("error creating unzip destination directory")
	}

	go func() {
		defer close(out)

		for payload := range in {
			payloadMap := payload.Value.(map[string]interface{})

			collection := payloadMap["collection"].(*Collection)
			pathOfFileWithinZip := payloadMap["pathOfFileWithinZip"].(string)
			pathOfZipFile := payloadMap["pathOfZipFile"].(string)
			unzippedDestinationPath := payloadMap["unzippedDestinationPath"].(string)

			if _, err := os.Stat(unzippedDestinationPath); err != nil {
				if err := unzip(pathOfZipFile, pathOfFileWithinZip, unzippedDestinationPath); err != nil {
					log.WithError(err).Fatalf("error unzipping file %s\n", pathOfZipFile)
				}
			} else {
				log.Infof("%q already extracted to %q", pathOfZipFile, unzippedDestinationPath)
			}

			out <- &ProcessingPayload{
				Value: &extractFileResponse{
					collection:        collection,
					extractedFilePath: unzippedDestinationPath,
				},
			}
		}
	}()

	return out
}

func unzip(pathOfZipFile, pathOfFileWithinZip, unzippedFileDestinationPath string) error {
	r, err := zip.OpenReader(pathOfZipFile)
	if err != nil {
		log.WithError(err).Error("error opening zip file")
		return err
	}
	defer r.Close()

	for _, zippedFile := range r.File {
		rc, err := zippedFile.Open()
		if err != nil {
			log.WithError(err).Error("error opening file within zipped file")
			return err
		}
		defer rc.Close()

		log.Debugf("should we unzip %q? %v\n", zippedFile.Name, zippedFile.Name == pathOfFileWithinZip)
		if zippedFile.Name != pathOfFileWithinZip {
			continue
		}

		f, err := os.OpenFile(unzippedFileDestinationPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, zippedFile.Mode())
		if err != nil {
			log.WithError(err).Error("error allocating unzipped file")
			return err
		}
		defer f.Close()

		if _, err := io.Copy(f, rc); err != nil {
			log.WithError(err).Error("error writing unzipped file")
			return err
		}

		log.Debugf("unzipped file at %q\n", unzippedFileDestinationPath)
		break
	}

	return nil
}
