import pandas as pd
import numpy as np
from math import ceil

class GamificationPricingCalculator:
    def __init__(self):
        # Industry benchmarks and assumptions
        self.benchmarks = {
            # Conversion rates
            'baseline_conversion_rate': 0.02,  # 2% industry average for tour operators
            'gamification_lift': 0.40,  # 40% improvement with gamification
            'direct_booking_premium': 0.30,  # 30% more profit on direct vs OTA bookings
            
            # OTA commission rates
            'ota_commission_rates': {
                'getyourguide': 0.30,
                'viator': 0.25,
                'tripadvisor': 0.25,
                'expedia': 0.20,
                'klook': 0.22,
                'tiqets': 0.25,
                'average': 0.25
            },
            
            # Tour operator metrics
            'avg_tour_price': 85,  # USD average tour price
            'price_ranges': {
                'budget': (25, 60),
                'mid_range': (60, 120),
                'premium': (120, 300),
                'luxury': (300, 800)
            },
            
            # Implementation complexity factors
            'complexity_multipliers': {
                'simple': 1.0,      # Basic tour selection quiz
                'moderate': 1.5,    # Multi-step recommendation engine
                'complex': 2.0,     # Advanced personalization + integrations
                'enterprise': 3.0   # Custom features + multiple integrations
            }
        }
        
        # Base pricing tiers
        self.base_pricing = {
            'setup_fee_base': 2500,  # Base implementation fee
            'monthly_base': 150,     # Base monthly fee
            'traffic_tiers': {
                (0, 1000): {'multiplier': 0.6, 'name': 'Starter'},
                (1000, 5000): {'multiplier': 1.0, 'name': 'Growth'},
                (5000, 15000): {'multiplier': 1.8, 'name': 'Professional'},
                (15000, 50000): {'multiplier': 3.2, 'name': 'Business'},
                (50000, float('inf')): {'multiplier': 5.5, 'name': 'Enterprise'}
            }
        }

    def calculate_traffic_tier(self, monthly_visits):
        """Determine pricing tier based on monthly traffic."""
        for (min_traffic, max_traffic), tier_info in self.base_pricing['traffic_tiers'].items():
            if min_traffic <= monthly_visits < max_traffic:
                return tier_info
        return self.base_pricing['traffic_tiers'][(0, 1000)]  # Fallback

    def estimate_tour_price_category(self, booking_tech, ota_deps, country=None):
        """Estimate tour price category based on booking technology and market."""
        score = 0
        
        # Booking technology sophistication
        premium_tech = ['fareharbor', 'regiondo', 'trekksoft', 'bokun']
        mid_tech = ['bookeo', 'checkfront', 'rezdy', 'shopify']
        
        if any(tech in booking_tech for tech in premium_tech):
            score += 3
        elif any(tech in booking_tech for tech in mid_tech):
            score += 2
        elif 'woocommerce' in booking_tech or 'stripe' in booking_tech:
            score += 1
        
        # OTA presence (premium OTAs = higher price point)
        premium_otas = ['viator', 'getyourguide', 'tripadvisor']
        if any(ota in ota_deps for ota in premium_otas):
            score += 2
        
        # Market indicators (if available)
        premium_markets = ['US', 'UK', 'AU', 'CH', 'NO', 'DK']
        mid_markets = ['CA', 'DE', 'FR', 'NL', 'SE']
        
        if country in premium_markets:
            score += 2
        elif country in mid_markets:
            score += 1
        
        # Categorize based on score
        if score >= 6:
            return 'luxury', self.benchmarks['price_ranges']['luxury']
        elif score >= 4:
            return 'premium', self.benchmarks['price_ranges']['premium']
        elif score >= 2:
            return 'mid_range', self.benchmarks['price_ranges']['mid_range']
        else:
            return 'budget', self.benchmarks['price_ranges']['budget']

    def calculate_ota_commission_savings(self, monthly_visits, avg_tour_price, ota_dependencies):
        """Calculate potential savings from reducing OTA dependency."""
        # Estimate current bookings
        baseline_conversion = self.benchmarks['baseline_conversion_rate']
        monthly_conversions = monthly_visits * baseline_conversion
        
        # Estimate OTA vs direct booking split
        if ota_dependencies and ota_dependencies != 'None detected':
            ota_booking_percentage = 0.60  # 60% through OTAs if dependent
        else:
            ota_booking_percentage = 0.30  # 30% through OTAs if not heavily dependent
        
        ota_bookings = monthly_conversions * ota_booking_percentage
        
        # Calculate average commission rate
        detected_otas = []
        if isinstance(ota_dependencies, str):
            for ota, rate in self.benchmarks['ota_commission_rates'].items():
                if ota in ota_dependencies.lower():
                    detected_otas.append(rate)
        
        avg_commission_rate = np.mean(detected_otas) if detected_otas else self.benchmarks['ota_commission_rates']['average']
        
        # Current monthly loss to commissions
        monthly_commission_loss = ota_bookings * avg_tour_price * avg_commission_rate
        
        return {
            'monthly_ota_bookings': ota_bookings,
            'avg_commission_rate': avg_commission_rate,
            'monthly_commission_loss': monthly_commission_loss,
            'annual_commission_loss': monthly_commission_loss * 12,
            'ota_dependency_level': 'High' if ota_booking_percentage > 0.5 else 'Medium'
        }

    def calculate_gamification_value(self, monthly_visits, avg_tour_price, current_conversion_rate=None):
        """Calculate the value of gamification implementation."""
        if current_conversion_rate is None:
            current_conversion_rate = self.benchmarks['baseline_conversion_rate']
        
        # Current performance
        current_monthly_bookings = monthly_visits * current_conversion_rate
        current_monthly_revenue = current_monthly_bookings * avg_tour_price
        
        # With gamification (40% conversion lift)
        improved_conversion_rate = current_conversion_rate * (1 + self.benchmarks['gamification_lift'])
        improved_monthly_bookings = monthly_visits * improved_conversion_rate
        improved_monthly_revenue = improved_monthly_bookings * avg_tour_price
        
        # Additional value calculations
        additional_monthly_bookings = improved_monthly_bookings - current_monthly_bookings
        additional_monthly_revenue = improved_monthly_revenue - current_monthly_revenue
        
        return {
            'current_monthly_bookings': current_monthly_bookings,
            'current_monthly_revenue': current_monthly_revenue,
            'improved_monthly_bookings': improved_monthly_bookings,
            'improved_monthly_revenue': improved_monthly_revenue,
            'additional_monthly_bookings': additional_monthly_bookings,
            'additional_monthly_revenue': additional_monthly_revenue,
            'additional_annual_revenue': additional_monthly_revenue * 12,
            'conversion_improvement': f"{self.benchmarks['gamification_lift']*100:.0f}%",
            'roi_payback_months': None  # Will be calculated after pricing
        }

    def calculate_pricing(self, monthly_visits, complexity='moderate', custom_features=False):
        """Calculate pricing based on traffic and complexity."""
        # Get traffic tier
        tier_info = self.calculate_traffic_tier(monthly_visits)
        
        # Base pricing
        setup_fee = self.base_pricing['setup_fee_base'] * tier_info['multiplier']
        monthly_fee = self.base_pricing['monthly_base'] * tier_info['multiplier']
        
        # Complexity adjustment
        complexity_multiplier = self.benchmarks['complexity_multipliers'].get(complexity, 1.5)
        setup_fee *= complexity_multiplier
        
        # Custom features premium
        if custom_features:
            setup_fee *= 1.3
            monthly_fee *= 1.2
        
        # Round to reasonable numbers
        setup_fee = ceil(setup_fee / 100) * 100  # Round to nearest $100
        monthly_fee = ceil(monthly_fee / 25) * 25   # Round to nearest $25
        
        return {
            'tier_name': tier_info['name'],
            'setup_fee': setup_fee,
            'monthly_fee': monthly_fee,
            'annual_fee': monthly_fee * 12,
            'total_year_1': setup_fee + (monthly_fee * 12),
            'complexity_level': complexity,
            'traffic_multiplier': tier_info['multiplier']
        }

    def generate_value_proposition(self, company_data):
        """Generate complete value proposition for a company."""
        # Extract data
        monthly_visits = company_data.get('monthly_visits', 2000)
        booking_tech = company_data.get('booking_technology', '')
        ota_deps = company_data.get('ota_dependencies', '')
        company_name = company_data.get('company_name', 'Your Company')
        
        # Estimate tour price category
        price_category, price_range = self.estimate_tour_price_category(booking_tech, ota_deps)
        avg_tour_price = np.mean(price_range)
        
        # Calculate OTA savings
        ota_analysis = self.calculate_ota_commission_savings(monthly_visits, avg_tour_price, ota_deps)
        
        # Calculate gamification value
        gamification_value = self.calculate_gamification_value(monthly_visits, avg_tour_price)
        
        # Calculate pricing
        pricing = self.calculate_pricing(monthly_visits, complexity='moderate')
        
        # Calculate ROI
        total_annual_benefit = gamification_value['additional_annual_revenue'] + ota_analysis['annual_commission_loss'] * 0.25  # 25% reduction in OTA dependency
        roi_percentage = (total_annual_benefit / pricing['total_year_1']) * 100
        payback_months = pricing['total_year_1'] / (total_annual_benefit / 12)
        
        return {
            'company_name': company_name,
            'monthly_visits': monthly_visits,
            'price_category': price_category,
            'avg_tour_price': avg_tour_price,
            'ota_analysis': ota_analysis,
            'gamification_value': gamification_value,
            'pricing': pricing,
            'roi_analysis': {
                'total_annual_benefit': total_annual_benefit,
                'roi_percentage': roi_percentage,
                'payback_months': payback_months,
                'break_even_year_1': total_annual_benefit > pricing['total_year_1']
            }
        }

    def create_proposal_summary(self, value_prop):
        """Create a formatted proposal summary."""
        vp = value_prop
        
        summary = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                        GAMIFICATION VALUE PROPOSAL                           ║
║                              {vp['company_name']:<40}                       ║
╚══════════════════════════════════════════════════════════════════════════════╝

📊 CURRENT SITUATION:
   • Monthly Website Visitors: {vp['monthly_visits']:,}
   • Estimated Tour Price Range: ${vp['avg_tour_price']:.0f} ({vp['price_category']} market)
   • Current Monthly Bookings: ~{vp['gamification_value']['current_monthly_bookings']:.0f}
   • Current Monthly Revenue: ${vp['gamification_value']['current_monthly_revenue']:,.0f}

💸 OTA COMMISSION ANALYSIS:
   • Monthly Loss to Commissions: ${vp['ota_analysis']['monthly_commission_loss']:,.0f}
   • Annual Loss to Commissions: ${vp['ota_analysis']['annual_commission_loss']:,.0f}
   • Average Commission Rate: {vp['ota_analysis']['avg_commission_rate']*100:.0f}%
   • OTA Dependency Level: {vp['ota_analysis']['ota_dependency_level']}

🎯 GAMIFICATION IMPACT:
   • Conversion Rate Improvement: +{vp['gamification_value']['conversion_improvement']}
   • Additional Monthly Bookings: +{vp['gamification_value']['additional_monthly_bookings']:.0f}
   • Additional Monthly Revenue: +${vp['gamification_value']['additional_monthly_revenue']:,.0f}
   • Additional Annual Revenue: +${vp['gamification_value']['additional_annual_revenue']:,.0f}

💰 INVESTMENT & PRICING:
   • Package Tier: {vp['pricing']['tier_name']}
   • One-Time Setup Fee: ${vp['pricing']['setup_fee']:,}
   • Monthly Management: ${vp['pricing']['monthly_fee']:,}
   • Total Year 1 Investment: ${vp['pricing']['total_year_1']:,}

📈 ROI ANALYSIS:
   • Total Annual Benefit: ${vp['roi_analysis']['total_annual_benefit']:,.0f}
   • Return on Investment: {vp['roi_analysis']['roi_percentage']:.0f}%
   • Payback Period: {vp['roi_analysis']['payback_months']:.1f} months
   • Profitable in Year 1: {'✅ YES' if vp['roi_analysis']['break_even_year_1'] else '❌ NO'}

🎯 VALUE PROPOSITION:
   Every month you delay costs you ${vp['gamification_value']['additional_monthly_revenue']:,.0f} 
   in missed direct bookings and ${vp['ota_analysis']['monthly_commission_loss']:,.0f} in OTA commissions.
   
   Our gamified chatbot pays for itself in {vp['roi_analysis']['payback_months']:.1f} months and delivers
   ${vp['roi_analysis']['total_annual_benefit']:,.0f} in additional annual profit.
"""
        return summary

def process_prospect_csv(csv_file, output_file):
    """Process a CSV of prospects and generate value propositions."""
    calculator = GamificationPricingCalculator()
    
    # Load data
    df = pd.read_csv(csv_file)
    
    results = []
    
    for index, row in df.iterrows():
        # Extract relevant data
        company_data = {
            'company_name': row.get('Company Name', f'Company {index+1}'),
            'monthly_visits': row.get('Monthly_Visits', row.get('monthly_visits', 2000)),
            'booking_technology': str(row.get('booking_technology_detailed', '')),
            'ota_dependencies': str(row.get('ota_dependencies_detailed', ''))
        }
        
        # Generate value proposition
        value_prop = calculator.generate_value_proposition(company_data)
        
        # Flatten for CSV output
        result_row = {
            'Company_Name': value_prop['company_name'],
            'Monthly_Visits': value_prop['monthly_visits'],
            'Price_Category': value_prop['price_category'],
            'Avg_Tour_Price': value_prop['avg_tour_price'],
            'Current_Monthly_Revenue': value_prop['gamification_value']['current_monthly_revenue'],
            'Additional_Monthly_Revenue': value_prop['gamification_value']['additional_monthly_revenue'],
            'Additional_Annual_Revenue': value_prop['gamification_value']['additional_annual_revenue'],
            'Annual_OTA_Loss': value_prop['ota_analysis']['annual_commission_loss'],
            'Setup_Fee': value_prop['pricing']['setup_fee'],
            'Monthly_Fee': value_prop['pricing']['monthly_fee'],
            'Total_Year_1_Cost': value_prop['pricing']['total_year_1'],
            'Total_Annual_Benefit': value_prop['roi_analysis']['total_annual_benefit'],
            'ROI_Percentage': value_prop['roi_analysis']['roi_percentage'],
            'Payback_Months': value_prop['roi_analysis']['payback_months'],
            'Profitable_Year_1': value_prop['roi_analysis']['break_even_year_1']
        }
        
        results.append(result_row)
    
    # Save results
    results_df = pd.DataFrame(results)
    results_df.to_csv(output_file, index=False)
    
    print(f"✅ Value propositions generated for {len(results)} companies")
    print(f"📁 Results saved to: {output_file}")
    
    # Print summary statistics
    profitable_companies = len(results_df[results_df['Profitable_Year_1'] == True])
    avg_roi = results_df['ROI_Percentage'].mean()
    avg_payback = results_df['Payback_Months'].mean()
    
    print(f"\n📊 SUMMARY STATISTICS:")
    print(f"   Profitable in Year 1: {profitable_companies}/{len(results)} companies ({profitable_companies/len(results)*100:.1f}%)")
    print(f"   Average ROI: {avg_roi:.0f}%")
    print(f"   Average Payback: {avg_payback:.1f} months")
    
    return results_df

def main():
    """Main function for value calculation."""
    print("💰 GAMIFICATION PRICING & VALUE CALCULATOR")
    print("=" * 60)
    
    choice = input("Choose option:\n1. Single company analysis\n2. Process CSV file\nEnter choice (1 or 2): ").strip()
    
    if choice == '1':
        # Single company analysis
        company_name = input("Company name: ").strip()
        monthly_visits = int(input("Monthly website visits: ").strip())
        booking_tech = input("Booking technology (optional): ").strip()
        ota_deps = input("OTA dependencies (optional): ").strip()
        
        calculator = GamificationPricingCalculator()
        company_data = {
            'company_name': company_name,
            'monthly_visits': monthly_visits,
            'booking_technology': booking_tech,
            'ota_dependencies': ota_deps
        }
        
        value_prop = calculator.generate_value_proposition(company_data)
        proposal = calculator.create_proposal_summary(value_prop)
        
        print(proposal)
        
        # Save to file
        with open(f"{company_name.replace(' ', '_')}_value_proposal.txt", 'w') as f:
            f.write(proposal)
        
    elif choice == '2':
        # CSV processing
        csv_file = input("Enter CSV filename: ").strip()
        output_file = input("Output filename (default: value_propositions.csv): ").strip() or 'value_propositions.csv'
        
        process_prospect_csv(csv_file, output_file)
    
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()