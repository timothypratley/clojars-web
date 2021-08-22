(ns clojars.web.browse
  (:require
   [clojars.db :refer [browse-projects count-all-projects
                       count-projects-before]]
   [clojars.web.common :refer [html-doc jar-link user-link format-date
                               page-nav page-description
                               collection-fork-notice
                               verified-group-badge]]
   [clojars.web.structured-data :as structured-data]
   [hiccup.form :refer [label submit-button text-field submit-button]]
   [ring.util.response :refer [redirect]]))

(defn browse-page [db account page per-page]
  (let [project-count (count-all-projects db)
        total-pages (-> (/ project-count per-page) Math/ceil .intValue)
        projects (browse-projects db account page per-page)]
    (html-doc
     "All projects" {:account account :description "Browse all of the projects in Clojars"}
     [:div.light-article.row
      (structured-data/breadcrumbs [{:name "All projects" :url "https://clojars.org/projects"}])
      [:h1 "All projects"]
      [:div.small-section
       [:form.browse-from {:method :get :action "/projects"}
        (label :from "Enter a few letters...")
        (text-field {:placeholder "Enter a few letters..."
                     :required true}
                    :from)
        (submit-button {:id :jump} "Jump")]]
      collection-fork-notice
      (page-description page per-page project-count)
      [:ul.row
       (for [[i jar] (map-indexed vector projects)]
         [:li.col-xs-12.col-sm-6.col-md-4.col-lg-3
          [:div.result
           [:a {:name i}]
           [:div
            [:div (jar-link jar) " " (:version jar)]]
           (when (:verified-group? jar)
             [:div verified-group-badge])
           [:br]
           (if (seq (:description jar))
             [:span.desc (:description jar)]
             [:span.hint "No description given"])
           [:hr]
           [:span.details
            (user-link (:user jar))
            " "
            (when-let [created (:created jar)]
              [:td (format-date created)])]]])]
      (page-nav page total-pages)])))

(defn browse [db account params]
  (let [per-page 20]
    (if-let [from (:from params)]
      (let [i (count-projects-before db from)
            page (inc (int (/ i per-page)))]
        (redirect (str "/projects?page=" page "#" (mod i per-page))))
      (browse-page db account (or (:page params) 1) per-page))))
