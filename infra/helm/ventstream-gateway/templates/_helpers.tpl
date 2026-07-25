{{- define "gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "gateway.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "ventstream-%s" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "gateway.labels" -}}
app.kubernetes.io/name: {{ include "gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: gateway
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "gateway.selectorLabels" -}}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: gateway
{{- end -}}

{{- define "gateway.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end -}}

{{- define "gateway.validate" -}}
{{- if and .Values.image.digest (not (regexMatch "^sha256:[0-9a-f]{64}$" .Values.image.digest)) -}}{{- fail "image.digest must be a lowercase sha256 digest" -}}{{- end -}}
{{- end -}}

{{/* Whether each role is enabled (ws/graphql are not substrings of each other). */}}
{{- define "gateway.hasWs" -}}{{- if contains "ws" .Values.roles -}}true{{- end -}}{{- end -}}
{{- define "gateway.hasGraphql" -}}{{- if contains "graphql" .Values.roles -}}true{{- end -}}{{- end -}}

{{/* ConfigMap that carries the GraphQL SDL schema, if provided inline. */}}
{{- define "gateway.schemaConfigMap" -}}
{{- if .Values.graphql.schema.existingConfigMap -}}{{ .Values.graphql.schema.existingConfigMap }}{{- else -}}{{ printf "%s-schema" (include "gateway.fullname" .) }}{{- end -}}
{{- end -}}

{{- define "gateway.schemaKey" -}}
{{- if .Values.graphql.schema.existingConfigMap -}}{{ .Values.graphql.schema.existingConfigMapKey }}{{- else -}}subscriptions.graphql{{- end -}}
{{- end -}}

{{/* True when a GraphQL SDL schema is available (inline or existing CM). */}}
{{- define "gateway.hasSchema" -}}
{{- if and (include "gateway.hasGraphql" .) (or .Values.graphql.schema.inline .Values.graphql.schema.existingConfigMap) -}}true{{- end -}}
{{- end -}}
