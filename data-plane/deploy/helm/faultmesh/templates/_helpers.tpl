{{/* Fully qualified name for a component */}}
{{- define "faultmesh.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "faultmesh.componentName" -}}
{{- printf "%s-%s" (include "faultmesh.fullname" .root) .component | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "faultmesh.image" -}}
{{- $reg := .root.Values.imageRegistry -}}
{{- if $reg -}}
{{ $reg }}/{{ .img.repository }}:{{ .img.tag }}
{{- else -}}
{{ .img.repository }}:{{ .img.tag }}
{{- end -}}
{{- end -}}

{{- define "faultmesh.labels" -}}
app.kubernetes.io/name: faultmesh
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
