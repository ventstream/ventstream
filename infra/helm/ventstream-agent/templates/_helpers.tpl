{{- define "agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "agent.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "ventstream-%s" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "agent.labels" -}}
app.kubernetes.io/name: {{ include "agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: cdc-agent
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "agent.tag" -}}
{{- .Values.image.tag | default .Chart.AppVersion -}}
{{- end -}}

{{- define "agent.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (include "agent.tag" .) -}}
{{- end -}}
{{- end -}}

{{- define "agent.validate" -}}
{{- if and .Values.image.digest (not (regexMatch "^sha256:[0-9a-f]{64}$" .Values.image.digest)) -}}{{- fail "image.digest must be a lowercase sha256 digest" -}}{{- end -}}
{{- end -}}

{{- define "agent.secretName" -}}
{{- if .Values.controlPlane.create -}}{{ printf "%s-secrets" (include "agent.fullname" .) }}{{- else -}}{{ required "controlPlane.existingSecret is required when controlPlane.create is false" .Values.controlPlane.existingSecret }}{{- end -}}
{{- end -}}

{{/* Secret key holding the source DB password. */}}
{{- define "agent.passwordKey" -}}
{{- if eq .Values.source.type "neo4j" -}}VS_NEO4J_PASSWORD{{- else -}}VS_PG_PASSWORD{{- end -}}
{{- end -}}

{{/* ConfigMap that carries the projection spec. */}}
{{- define "agent.specConfigMap" -}}
{{- if .Values.spec.existingConfigMap -}}{{ .Values.spec.existingConfigMap }}{{- else -}}{{ printf "%s-spec" (include "agent.fullname" .) }}{{- end -}}
{{- end -}}

{{- define "agent.specKey" -}}
{{- if .Values.spec.existingConfigMap -}}{{ .Values.spec.existingConfigMapKey }}{{- else -}}spec.yaml{{- end -}}
{{- end -}}
