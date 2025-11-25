import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import uuid
import re

# Configuration
INDEX_URL = "https://lwlies.com/reviews"
BASE_URL = "https://lwlies.com"
OUTPUT_FILE = 'data/raw/raw_Arthur.csv'

def get_review_links(index_url):
    """Récupère les URLs des critiques depuis la page d'index."""
    print(f"-> Récupération des liens depuis : {index_url}")
    links = set()
    
    try:
        response = requests.get(index_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # On cherche tous les liens qui contiennent '/reviews/'
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link['href']
            # Filtre : Doit contenir /reviews/ et ne pas être la page index elle-même
            if '/reviews/' in href and href.strip('/') != 'reviews':
                full_url = href if href.startswith('http') else BASE_URL + href
                links.add(full_url)
                
    except Exception as e:
        print(f"❌ Erreur Index: {e}")
        
    return list(links)

def scrape_review_page(url):
    """Extrait les données d'une page de critique spécifique."""
    print(f"   Scraping: {url}")
    
    data = {
        'review_id': str(uuid.uuid4()),
        'blog_name': 'Little White Lies',
        'source_url': url,
        'film_title': None,
        'review_date': None,     # Difficile à trouver sur ce site, on laissera vide ou à compléter
        'numerical_rating': None,
        'text_complete': None,
        'author': None,
        'cited_works_list': []
    }
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.content, 'html.parser')

        # 1. TITRE DU FILM (Séparation Titre/Critique)
        h1 = soup.find('h1')
        if h1:
            full_title = h1.get_text(strip=True)
            
            # On cherche l'index de "review" (ou " Review")
            # Ex: "Blue Moon review – one spry night..."
            if ' review' in full_title:
                title_end_index = full_title.lower().find(' review')
                data['film_title'] = full_title[:title_end_index].strip()
            elif ' Review' in full_title:
                title_end_index = full_title.find(' Review')
                data['film_title'] = full_title[:title_end_index].strip()
            else:
                data['film_title'] = full_title # Par défaut, on prend tout le titre si pas de mot clé

        # 2. AUTEUR
        # On cherche le lien <a> qui contient à la fois '/contributor/' dans son href
        # et les classes 'font-primary' et 'font-bold' pour éviter les liens standards.
        author_tag = soup.find('a', href=lambda x: x and '/contributor/' in x, 
                                class_=lambda x: x and 'font-primary' in x and 'font-bold' in x)
        
        if author_tag:
            data['author'] = author_tag.get_text(strip=True)
        else:
            data['author'] = 'N/A' # Marquer comme non trouvé si nécessaire

        # 3. NOTE (Moyenne des 3 bulles)
        # Basé sur ton snippet: class="w-8 h-8 rounded-full bg-black..."
        score_spans = soup.find_all(attrs={re.compile(r"^data-flatplan-review-score"): True})
        
        scores = []
        for tag in score_spans:
            text_score = tag.get_text(strip=True)
            if text_score.isdigit():
                scores.append(int(text_score))
        
        if scores:
            # Calcul de la moyenne (sur 5)
            # On vérifie qu'il y a bien 3 scores, sinon on prend tous ceux trouvés
            if len(scores) == 3: 
                average_score = sum(scores) / 3
            else:
                average_score = sum(scores) / len(scores)
                
            data['numerical_rating'] = round(average_score, 2)
            print(f"      -> Note trouvée : {data['numerical_rating']}/5 (basé sur {len(scores)} critères)")
        else:
            # Fallback : Si la méthode regex échoue, on recherche par la classe des conteneurs DIV
            # Ceci est utile si les attributs data-flatplan ne sont pas chargés correctement.
            
            # Recherche des DIV conteneurs avec la classe unique fournie :
            rating_divs = soup.find_all(
                'div', 
                class_=lambda x: x and 'w-14 h-14 rounded-full bg-black' in x
            )
            
            fallback_scores = []
            for div in rating_divs:
                # Chercher le <span> avec la note à l'intérieur du <div>
                score_span = div.find('span')
                if score_span and score_span.get_text(strip=True).isdigit():
                    fallback_scores.append(int(score_span.get_text(strip=True)))

            if fallback_scores:
                average_score = sum(fallback_scores) / len(fallback_scores)
                data['numerical_rating'] = round(average_score, 2)
                print(f"      -> Note trouvée par Fallback : {data['numerical_rating']}/5 (basé sur {len(fallback_scores)} critères)")
            else:
                print("      -> Pas de note trouvée.")

        # 4. DATE DE PUBLICATION (Correction pour les espaces/sauts de ligne)
        
        # On cible directement le SPAN qui contient la date, en utilisant ses classes uniques
        date_span = soup.find('span', class_=lambda x: x and 'uppercase' in x and 'font-primary' in x and 'font-bold' in x and 'not-italic' in x)
        
        if date_span:
            # Pour vérifier qu'il s'agit bien de la date et non de l'auteur (qui a des classes similaires), 
            # on vérifie que l'élément parent n'est pas un lien <a>.
            # La date est dans un <p> qui contient 'Published', l'auteur est dans un <a>.
            
            # Méthode plus simple : on se fie uniquement aux classes du SPAN, car l'auteur est un <a>.
            parent_p = date_span.find_parent('p')
            
            # Vérifier que le paragraphe parent contient bien la chaîne "Published" pour confirmer que c'est la date
            if parent_p and 'Published' in parent_p.get_text():
                 data['review_date'] = date_span.get_text(strip=True)
            else:
                 data['review_date'] = 'N/A' # C'était l'auteur ou un autre span
        else:
            data['review_date'] = 'N/A'

        # 5. CORPS DU TEXTE (Nouvelle extraction robuste)
        
        # SÉLECTION DU CONTENEUR PRINCIPAL : On cherche la div qui contient le texte unique (text-prose ou column)
        content_div = soup.find('div', class_=lambda x: x and ('text-prose' in x or 'column' in x))
        
        if content_div:
            # A. NETTOYAGE : Supprimer les éléments perturbateurs (Get More et Pubs)
            # Cette étape reste la même et est cruciale.
            promo_box = content_div.find('div', class_=lambda x: x and 'bg-[var(--color-background-accent)]' in x)
            if promo_box:
                promo_box.decompose()

            ad_box = content_div.find('div', class_=lambda x: x and 'ad' in x)
            if ad_box:
                ad_box.decompose()
                
            # B. EXTRACTION : On prend tous les paragraphes restants
            paragraphs = content_div.find_all('p')
            
            # Joindre les paragraphes et s'assurer qu'il y a suffisamment de contenu
            clean_text = " ".join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
            
            if len(clean_text) > 50: 
                data['text_complete'] = clean_text
            else:
                data['text_complete'] = None
                print(f"      (⚠️ Corps du texte trop court ou non trouvé.)")
                
            # C. LINK ANALYSIS : Trouver les films cités (en italique <i>)
            cited_tags = content_div.find_all('i')
            # ... (gardez la même logique pour les œuvres citées) ...
            cited_works = [tag.get_text(strip=True) for tag in cited_tags if tag.get_text(strip=True) != data['film_title']]
            data['cited_works_list'] = ", ".join(set(cited_works))
        else:
            data['text_complete'] = None

    except Exception as e:
        print(f"⚠️ Erreur Scraping {url}: {e}")
        return None

    return data

# --- EXÉCUTION ---
if __name__ == "__main__":
    # 1. Récupérer les liens
    links = get_review_links(INDEX_URL)
    print(f"Trouvé {len(links)} critiques potentielles.")
    
    # Pour le test, on en prend que 10. Enlève [:10] pour tout scraper.
    links_to_scrape = links[:10] 
    
    results = []
    
    # 2. Scraper chaque page
    for i, link in enumerate(links_to_scrape):
        print(f"[{i+1}/{len(links_to_scrape)}]", end=" ")
        review_data = scrape_review_page(link)
        
        # CONDITION DE SAUVEGARDE AMÉLIORÉE :
        # On sauvegarde si le scraping a réussi (review_data n'est pas None)
        # ET si on a pu extraire au moins le titre du film.
        if review_data and review_data['film_title']: 
            results.append(review_data)
        elif review_data:
            # Afficher un message si le titre manque pour cette critique
            print(f" (⚠️ Non sauvegardé: Titre du film manquant pour {link})") 
        
        time.sleep(0.5) # Politesse : attendre 1 seconde entre chaque requêtequête
        
    # 3. Sauvegarder
    if results:
        df = pd.DataFrame(results)
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ Succès ! {len(df)} critiques sauvegardées dans {OUTPUT_FILE}")
    else:
        print("\n❌ Aucune donnée récupérée.")