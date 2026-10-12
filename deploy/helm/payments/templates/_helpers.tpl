{{- define "payments.labels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/part-of: payments
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/version: {{ .root.Values.image.tag | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version }}
{{- end }}

{{- define "payments.selector" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end }}

{{- define "payments.image" -}}
{{ printf "%s/%s:%s" .root.Values.image.repository .command (required "image.tag is required: the commit the images were built from (make k8s-deploy sets it)" .root.Values.image.tag) }}
{{- end }}

{{- define "payments.databaseURL" -}}
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secrets.database.name }}
      key: {{ .Values.secrets.database.key }}
{{- end }}
