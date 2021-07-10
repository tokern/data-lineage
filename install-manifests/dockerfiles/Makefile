default: all

.PHONY: default all fetch_dump

date := `date '+%Y-%m-%d'`
TARGET_IMAGE ?= demo-catalog
VERSION ?= "0.2.0"
DESTINATION_REPOSITORY ?= "tokern"

all: generate_image push_to_registry finished

 check_vars:
	@test -n "$DESTINATION_REPOSITORY" || (echo "You need to set DESTINATION_REPOSITORY environment variable" >&2 && exit 1)

generate_image:
	@docker build . -f Dockerfile-$(TARGET_IMAGE) -t $(TARGET_IMAGE)\:latest -t $(DESTINATION_REPOSITORY)/$(TARGET_IMAGE)\:latest -t $(DESTINATION_REPOSITORY)/$(TARGET_IMAGE)\:$(VERSION)

push_to_registry:
	@echo ""
	@echo "====== Pushing image to repository ======"
	@docker push $(DESTINATION_REPOSITORY)/$(TARGET_IMAGE):latest
	@docker push $(DESTINATION_REPOSITORY)/$(TARGET_IMAGE):$(VERSION)

finished:
	@echo ""
	@echo "Finished with success. Pushed image to $(DESTINATION_REPOSITORY)/$(TARGET_IMAGE)"