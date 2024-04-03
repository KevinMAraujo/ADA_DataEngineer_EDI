from flask import Flask, request, jsonify
import modules.api as api
from modules.utils import get_env_var

app = Flask(__name__)

@app.route("/NewsApi/get_everything",methods=["POST"])
def webhook():
    data = request.json

    # Lista de chaves esperadas
    chaves_esperadas = ['q', 'searchIn', 'sources', 'domains', 'excludeDomains',
                     'from', 'to', 'language', 'sortBy', 'pageSize', 'page']
    
    # Verifica se há chaves inválidas
    chaves_invalidas = [key for key in data if key not in chaves_esperadas]
    if chaves_invalidas:
        return jsonify({"error": f"Chaves inválidas: {', '.join(chaves_invalidas)}"}), 400
    
    # Renomear as chaves 'from' e 'to' para evitar conflito com palavras reservadas
    if 'from' in data:
        data['_from'] = data.pop('from')
    if 'to' in data:
        data['_to'] = data.pop('to')

    try:
        api_key = get_env_var(group='api_key')
        response = api.get_everything(
            apiKey=api_key,
            **data
        )

        return jsonify(response), 200
    except Exception as e:
        return jsonify({
            "Error": f"Erro ao efetuar a requisição {e}"
        }), 400
    
if __name__ == "__main__":
    app.run()