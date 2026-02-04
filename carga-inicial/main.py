import random
import requests
from datetime import date

from faker import Faker

# --- CONFIGURAÇÕES ---
BASE_URL = "https://magento.test"
ADMIN_TOKEN = "TOKEN_DE_ADMIN_AQUI"
LOCALE = "pt_BR"

# Configuração de Carga
TOTAL_CUSTOMERS = 30
TOTAL_ORDERS = 100  # Aumentei para ver melhor o mapa
MAX_ITEMS_PER_ORDER = 4

# Pesos para simular realidade demográfica (Sudeste com mais vendas)
# Os estados não listados terão peso 1 (mínimo)
REGION_WEIGHTS = {
    "SP": 40,
    "RJ": 20,
    "MG": 15,
    "RS": 10,
    "PR": 8,
    "SC": 7,
    "BA": 5,
    "PE": 4,
    "DF": 4,
    "GO": 3,
}

# Configuração de Gênero 
# 1 = Masculino, 2 = Feminino (padrão Magento)
GENDER_WEIGHTS = {
    1: 45,  # 45% Masculino
    2: 55,  # 55% Feminino
}

# Configuração de Faixa Etária 
AGE_DISTRIBUTION = {
    (18, 24): 15,  # 15% - Jovens adultos
    (25, 34): 30,  # 30% - Millennials (maior poder de compra online)
    (35, 44): 25,  # 25% - Geração X
    (45, 54): 15,  # 15% - Adultos maduros
    (55, 65): 10,  # 10% - Pré-aposentadoria
    (66, 80): 5,   # 5%  - Idosos
}

# --- SETUP ---
fake = Faker(LOCALE)
headers = {"Authorization": f"Bearer {ADMIN_TOKEN}", "Content-Type": "application/json"}


class MagentoGeoSeeder:
    def __init__(self):
        self.skus = []
        self.customer_ids = []
        self.regions_map = []  # Lista de regiões reais do Magento

    def log(self, msg):
        print(f"[LOG] {msg}")

    def _get_weighted_gender(self):
        """
        Retorna gênero baseado na distribuição configurada. 
        1 = Masculino, 2 = Feminino (padrão Magento)
        """
        population = []
        for gender_id, weight in GENDER_WEIGHTS.items():
            population. extend([gender_id] * weight)
        return random.choice(population)

    def _get_weighted_dob(self):
        """
        Gera data de nascimento baseada na distribuição etária configurada. 
        Retorna string no formato YYYY-MM-DD (padrão Magento).
        """
        # Seleciona faixa etária com peso
        population = []
        for age_range, weight in AGE_DISTRIBUTION.items():
            population.extend([age_range] * weight)
        
        min_age, max_age = random.choice(population)
        
        # Calcula ano de nascimento baseado na idade
        today = date.today()
        age = random.randint(min_age, max_age)
        birth_year = today.year - age
        
        # Gera mês e dia aleatórios
        birth_month = random.randint(1, 12)
        # Trata fevereiro e meses com 30 dias
        if birth_month == 2:
            max_day = 28
        elif birth_month in [4, 6, 9, 11]:
            max_day = 30
        else:
            max_day = 31
        birth_day = random.randint(1, max_day)
        
        return f"{birth_year}-{birth_month:02d}-{birth_day:02d}"

    def fetch_brazil_regions(self):
        """
        Busca os IDs reais das regiões (Estados) no Magento.
        Isso evita erros de Foreign Key e IDs hardcoded.
        """
        self.log("Mapeando regiões do Brasil no Magento...")
        resp = requests.get(
            f"{BASE_URL}/rest/V1/directory/countries/BR", headers=headers, verify=False
        )

        if resp.status_code == 200:
            data = resp.json()
            # O retorno contém a chave 'available_regions'
            if "available_regions" in data:
                self.regions_map = data["available_regions"]
                self.log(f"Carregadas {len(self.regions_map)} regiões (UFs).")
            else:
                raise Exception(
                    "Não foram encontradas regiões para o país BR. Verifique as configs do Magento."
                )
        else:
            raise Exception(f"Erro ao buscar regiões: {resp.text}")

    def get_weighted_random_region(self):
        """
        Seleciona um estado baseado na probabilidade demográfica.
        Retorna o objeto region completo (id, code, name).
        """
        # Cria uma lista onde estados com peso maior aparecem mais vezes
        population = []
        for region in self.regions_map:
            code = region["code"]  # Ex: SP, RJ
            weight = REGION_WEIGHTS.get(
                code, 1
            )  # Peso padrão 1 se não estiver na lista
            population.extend([region] * weight)

        return random.choice(population)

    def fetch_products(self):
        # Busca produtos simples (mesma lógica anterior)
        url = f"{BASE_URL}/rest/V1/products?searchCriteria[filter_groups][0][filters][0][field]=type_id&searchCriteria[filter_groups][0][filters][0][value]=simple&searchCriteria[pageSize]=100"
        resp = requests.get(url, headers=headers, verify=False)
        if resp.status_code == 200:
            self.skus = [item["sku"] for item in resp.json().get("items", [])]
            self.log(f"Skus carregados: {len(self.skus)}")
        else:
            self.log(f"Erro: {resp.status_code} - {resp.text}")

    def create_customers(self, count):
        # Cria clientes (mesma lógica anterior, omitida para brevidade)
        self.log(f"Criando {count} clientes...")
        for i in range(count):

            # Gera dados demográficos
            gender = self._get_weighted_gender()
            dob = self._get_weighted_dob()
            
            # Gera nome consistente com o gênero
            if gender == 1:
                first_name = fake.first_name_male()
            else:
                first_name = fake.first_name_female()

            payload = {
                "customer": {
                    "email": fake.unique.email(),
                    "firstname": fake.first_name(),
                    "lastname": fake.last_name(),
                    "store_id": 1,
                    "website_id": 1,
                    "dob" : dob,
                    "gender" : gender,
                },
                "password": "Password123!",
            }
            r = requests.post(
                f"{BASE_URL}/rest/V1/customers",
                json=payload,
                headers=headers,
                verify=False,
            )
            if r.status_code == 200:
                customer_data = r.json()
                self.customer_ids.append(customer_data["id"])
                gender_label = "M" if gender == 1 else "F"
                self. log(f"  [{i + 1}] Cliente #{customer_data['id']} | {first_name} | DOB: {dob} | Gênero: {gender_label}")
            else:
                self.log(f"  Erro ao criar cliente:  {r.text}")

    # ... (Métodos _create_cart_for_customer e _add_items_to_cart mantêm-se iguais) ...
    def _create_cart_for_customer(self, customer_id):
        r = requests.post(
            f"{BASE_URL}/rest/V1/customers/{customer_id}/carts",
            headers=headers,
            verify=False,
        )
        return r.json() if r.status_code == 200 else None

    def _add_items_to_cart(self, quote_id):
        num_items = random.randint(1, MAX_ITEMS_PER_ORDER)
        selected_skus = random.sample(self.skus, min(num_items, len(self.skus)))
        for sku in selected_skus:
            requests.post(
                f"{BASE_URL}/rest/V1/carts/{quote_id}/items",
                json={
                    "cartItem": {
                        "sku": sku,
                        "qty": random.randint(1, 2),
                        "quote_id": quote_id,
                    }
                },
                headers=headers,
                verify=False,
            )

    def _set_shipping_and_billing(self, quote_id):
        """
        Gera endereço completo e variado baseado na região sorteada.
        """
        # 1. Escolhe região com peso (Ex: 40% chance de ser SP)
        target_region = self.get_weighted_random_region()

        # 2. Gera cidade aleatória (Faker não garante cidade correta pro estado,
        # mas para BI o que importa é a consistência do Estado na tabela sales_order_address)
        city = fake.city()

        address_data = {
            "firstname": fake.first_name(),
            "lastname": fake.last_name(),
            "street": [
                fake.street_name(),
                str(fake.building_number()),
                fake.neighborhood(),
            ],
            "city": city,
            "country_id": "BR",
            "region_id": target_region["id"],  # O ID real do banco (Ex: 508)
            "region": target_region[
                "name"
            ],  # O Texto (Ex: São Paulo) - Importante para BI
            "region_code": target_region["code"],  # A Sigla (Ex: SP)
            "postcode": fake.postcode(),  # CEP aleatório formatado
            "telephone": fake.phone_number(),
        }

        payload = {
            "addressInformation": {
                "shipping_address": address_data,
                "billing_address": address_data,
                "shipping_carrier_code": "flatrate",
                "shipping_method_code": "flatrate",
            }
        }

        resp = requests.post(
            f"{BASE_URL}/rest/V1/carts/{quote_id}/shipping-information",
            json=payload,
            headers=headers,
            verify=False,
        )
        if resp.status_code != 200:
            print(f"Erro Endereço: {resp.text}")
        return resp.status_code == 200

    def _place_order(self, quote_id):
        payload = {"paymentMethod": {"method": "checkmo"}}
        r = requests.put(
            f"{BASE_URL}/rest/V1/carts/{quote_id}/order",
            json=payload,
            headers=headers,
            verify=False,
        )
        return r.json() if r.status_code == 200 else None

    def generate_orders(self, count):
        self.log(f"Gerando {count} pedidos distribuídos geograficamente...")
        for i in range(count):
            customer_id = random.choice(self.customer_ids)
            quote_id = self._create_cart_for_customer(customer_id)
            if quote_id:
                self._add_items_to_cart(quote_id)
                if self._set_shipping_and_billing(quote_id):
                    order_id = self._place_order(quote_id)
                    print(
                        f"  [{i + 1}] Pedido #{order_id} -> {fake.state_abbr()} (Simulado)"
                    )  # Apenas log visual


# --- RUN ---
if __name__ == "__main__":
    seeder = MagentoGeoSeeder()
    seeder.fetch_brazil_regions()  # Passo Crítico
    seeder.fetch_products()

    if seeder.skus and seeder.regions_map:
        seeder.create_customers(TOTAL_CUSTOMERS)
        seeder.generate_orders(TOTAL_ORDERS)
