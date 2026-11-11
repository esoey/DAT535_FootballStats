from statsbombpy import sb

def get_coordinates(stadium):
    """
    A function that gives you coordinates from all the stadiums participating in Laliga from Statsbomb per November, 2026.

    Input:
    ----- 
        stadium: string
            name of stadium which you need the coordinates to
    
    Return
    ------
        stadium_coordinates: tuple
            consists of latitude and longitude
    """
    stadium_coordinates = {
        "Abanca-Balaídos" : (42.211766, -8.739666),
        "Bernabéu" : (40.453044, -3.688197),
        "Campos de Sport de El Sardinero" : (43.476304, -3.79331),
        "Coliseum Alfonso Pérez" : (40.325725, -3.714933),
        "Estadi Municipal de Montilivi" : (41.961166, 2.828428),
        "Estadi Olímpic Lluís Companys" : (41.36472, 2.155645),
        "Estadio Abanca-Riazor" : (43.368716, -8.41751),
        "Estadio Alfredo Di Stéfano" : (40.47697, -3.614285),
        "Estadio Benito Villamarín" : (37.356406, -5.981739),
        "Estadio Carlos Belmonte" : (38.981082, -1.852144),
        "Estadio Ciudad de Valencia" : (39.494643, -0.363862),
        "Estadio El Alcoraz" : (42.13185, -0.42472),
        "Estadio El Sadar" : (42.7966625, -1.6371311),
        "Estadio Enrique Roca de Murcia" : (38.0422241, -1.1447324),
        "Estadio Heliodoro Rodríguez Lopéz" : (28.4632428, -16.260548),
        "Estadio José Rico Pérez" : (38.3573459, -0.4925974), 
        "Estadio La Rosaleda" : (36.73426080783186, -4.426581486641816), 
        "Estadio Manuel Martínez Valero" : (38.2670445, -0.6633051), 
        "Estadio Municipal El Molinón" : (43.536142696913366, -5.63743608626228), 
        "Estadio Municipal José Zorrilla" : (41.6444, -4.7611), 
        "Estadio Municipal de Butarque" : (40.340562, -3.760643), 
        "Estadio Municipal de Chapín" : (36.689343, -6.120523), 
        "Estadio Municipal de Ipurúa" : (43.1817, -2.4758), 
        "Estadio Nuevo Arcángel" : (37.87219, -4.764581), 
        "Estadio Nuevo Colombino" : (37.246663, -6.954204), 
        "Estadio Nuevo Los Cármenes" : (37.152891, -3.595894),
        "Estadio Nuevo Mirandilla" : (36.502604, -6.273017), 
        "Estadio Ramón Sánchez Pizjuán" : (37.384088, -5.970631), 
        "Estadio San Mamés" : (43.2642, -2.9494), 
        "Estadio Vicente Calderón" : (40.4017, -3.7206), # finnes ikke lengre
        "Estadio de Gran Canaria" : (28.100234, -15.456725), 
        "Estadio de Mendizorroza" : (42.8372, -2.6881), 
        "Estadio de Mestalla" : (39.4747, -0.3583), 
        "Estadio de Vallecas" : (40.391757, -3.658737), 
        "Estadio de la Cerámica" : (39.944208, -0.103405), 
        "Estadio de la Romareda" : (41.643034, -0.901014), 
        "Estádio Cívitas Metropolitano" : (40.4361, -3.5995), 
        "Los Pajaritos" : (41.7544, -2.4678), 
        "Nou Estadi Costa Daurada" : (41.12702, 1.272865), 
        "Power Horse Stadium – Estadio de los Juegos Mediterráneos" : (36.84, -2.4353), 
        "Reale Arena" : (43.30139, -1.973569), 
        "San Mamés Barria" : (43.264204, -2.949391), 
        "Spotify Camp Nou" : (41.380881, 2.122818), 
        "Stage Front Stadium" : (41.347847, 2.075603), 
        "Visit Mallorca Estadi" : (39.5900, 2.6300), 
    }
    return stadium_coordinates[stadium]
