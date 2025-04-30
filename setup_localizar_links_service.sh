#!/bin/bash

# Criar o arquivo de serviço
cat > /etc/systemd/system/localizar_links.service << EOL
[Unit]
Description=Serviço de busca de links para arquivos XML
After=network.target

[Service]
Type=oneshot
User=desenvolvimento
WorkingDirectory=/home/desenvolvimento/DownloadATF
ExecStart=/home/desenvolvimento/DownloadATF/venv/bin/python /home/desenvolvimento/DownloadATF/localizar_links_service.py 100
EOL

# Criar o arquivo de timer com 10 execuções diárias
cat > /etc/systemd/system/localizar_links.timer << EOL
[Unit]
Description=Executa o serviço de localizar links 10 vezes por dia

[Timer]
# Execução a cada 2 horas e 24 minutos (= 144 minutos), iniciando às 00:15
OnCalendar=*-*-* 00,02,04,07,09,12,14,16,19,21:15:00

# Garantir que as execuções perdidas não sejam compensadas
Persistent=false

[Install]
WantedBy=timers.target
EOL

# Recarregar configuração do systemd
systemctl daemon-reload

# Habilitar e iniciar o timer
systemctl enable localizar_links.timer
systemctl start localizar_links.timer

echo "Serviço e timer configurados com sucesso"
echo "O serviço será executado 10 vezes por dia nos seguintes horários:"
echo "00:15, 02:15, 04:15, 07:15, 09:15, 12:15, 14:15, 16:15, 19:15, 21:15"
echo ""
echo "Para verificar o status do timer: systemctl status localizar_links.timer"
echo "Para verificar as próximas execuções: systemctl list-timers"
echo "Para executar manualmente: systemctl start localizar_links.service"
