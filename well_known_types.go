package protoyaml

const (
	googlePackage = "google.protobuf"

	nullValueName     = "NullValue"
	nullValueFullName = googlePackage + "." + nullValueName

	anyName     = "Any"
	anyFullName = googlePackage + "." + anyName

	timestampName     = "Timestamp"
	timestampFullName = googlePackage + "." + timestampName

	durationName     = "Duration"
	durationFullName = googlePackage + "." + durationName

	emptyName     = "Empty"
	emptyFullName = googlePackage + "." + emptyName

	// TODO field_mask, struct.

	typeURLFieldName = "@type"
	invalidFieldName = "@invalid"
)
